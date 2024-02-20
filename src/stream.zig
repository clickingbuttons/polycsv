const std = @import("std");
const argparser = @import("argparser");
const TickerRegexes = @import("./Regex.zig").TickerRegexes;
const websocket = @import("websocket");
const key_var = @import("./polygon.zig").key_var;
const csv_mod = @import("./CsvWriter.zig");

const log = std.log;
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const FileWriter = gzip.Compressor(std.fs.File.Writer);

var gzipped: FileWriter = undefined;

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var opt = try argparser.parse(allocator, struct {
        help: bool = false,
        @"help-test-tickers": bool = false,
        @"test-tickers": []const u8 = "test_tickers.txt",
        output: []const u8 = "out.csv.gz",

        pub const __messages__ = .{
            .@"test-tickers" = "See help-test-tickers. ",
        };
        pub const __shorts__ = .{
            .help = .h,
            .output = .o,
        };
    }, "[topics]", null);
    defer opt.deinit();

    const args = opt.args;

    if (args.help or opt.positional_args.items.len == 0) {
        try opt.print_help(stderr.writer());
        return;
    } else if (args.@"help-test-tickers") {
        try stderr.writer().writeAll(TickerRegexes.description);
        return;
    }

    var ticker_regexes = try TickerRegexes.init(allocator, args.@"test-tickers");
    defer ticker_regexes.deinit();

    var out_file = try std.fs.cwd().createFile(args.output, .{});
    defer out_file.close();

    gzipped = try gzip.compressor(out_file.writer(), .{});

    // handle ctrl+c
    try std.os.sigaction(std.os.SIG.INT, &.{
        .handler = .{
            .handler = struct {
                fn func(sig: c_int) callconv(.C) void {
                    _ = sig;
                    _ = gzipped.write("\n") catch {};
                    gzipped.finish() catch {};
                    std.os.exit(0);
                }
            }.func,
        },
        .mask = std.os.empty_sigset,
        .flags = 0,
    }, null);

    const topics = try std.mem.join(allocator, ",", opt.positional_args.items);
    defer allocator.free(topics);

    var handler = try Handler.init(allocator, null, topics);
    defer handler.deinit();

    try handler.client.readLoop(&handler);
}

fn get_path(channel: []const u8) []const u8 {
    if (std.mem.startsWith(u8, channel, "XT.")) return "/crypto";

    return "/stocks";
}

// Order fields from least changing over time to most changing for better compression.
pub const Trade = struct {
    // ticker
    sym: []const u8,
    // tape
    z: u8,
    // sip timestamp
    t: usize,
    // price
    p: f64,
    // size
    s: u32,
    // conditions
    c: []u8 = &[_]u8{},
    // exchange
    x: u8,
    // id
    i: []const u8,
    // sequence number
    q: usize,
    // trf id
    trfi: u16 = 0,
    // trf timestamp
    trft: usize = 0,
};

const Handler = struct {
    const host = "socket.polygon.io";
    const port = 443;
    const Self = @This();

    client: websocket.Client,
    ca_bundle: std.crypto.Certificate.Bundle,
    allocator: std.mem.Allocator,
    writer: csv_mod.CsvWriter(Trade, FileWriter.Writer),

    pub fn init(allocator: std.mem.Allocator, api_key: ?[]const u8, channel: []const u8) !Handler {
        var writer = csv_mod.csvWriter(Trade, gzipped.writer());
        try writer.writeHeader();

        var ca_bundle = std.crypto.Certificate.Bundle{};
        try ca_bundle.rescan(allocator);
        errdefer ca_bundle.deinit(allocator);

        var env = try std.process.getEnvMap(allocator);
        defer env.deinit();
        const key = api_key orelse env.get(key_var) orelse return error.NoApiKey;

        log.debug("tls connect", .{});
        var client = try websocket.connect(allocator, host, port, .{
            .tls = true,
            .ca_bundle = ca_bundle,
        });

        log.debug("handshake", .{});
        try client.handshake(get_path(channel), .{ .timeout_ms = 5000, .headers = "host: " ++ host });

        const welcome = try client._reader.readMessage(&client.stream);
        log.debug("welcomed {s}", .{welcome.data});

        var send_buffer: [4096]u8 = undefined;
        var msg = try std.fmt.bufPrint(&send_buffer, "{{\"action\": \"auth\", \"params\": \"{s}\"}}", .{ key });
        try client.write(msg);

        const authed = try client._reader.readMessage(&client.stream);
        log.debug("authed {s}", .{ authed.data });

        msg = try std.fmt.bufPrint(&send_buffer, "{{\"action\": \"subscribe\", \"params\": \"{s}\"}}", .{ channel });
        try client.write(msg);

        const subscribed = try client._reader.readMessage(&client.stream);
        log.debug("subscribed {s}", .{ subscribed.data });

        return Self{
            .client = client,
            .ca_bundle = ca_bundle,
            .allocator = allocator,
            .writer = writer,
        };
    }

    pub fn deinit(self: *Self) void {
        self.ca_bundle.deinit(self.allocator);
        self.client.deinit();
    }

    pub fn handle(self: *Self, message: websocket.Message) !void {
        const allocator = self.allocator;

        const data = message.data;
        std.debug.print("msg {s}\n", .{data});
        const trades = try std.json.parseFromSlice([]Trade, allocator, data, .{ .ignore_unknown_fields = true });
        defer trades.deinit();

        for (trades.value) |t| try self.writer.writeRecord(t);
    }

    pub fn close(_: Self) void {}
};

test "parse" {
    const allocator = std.testing.allocator;
    const msg =
        \\ [{"ev":"T","sym":"HIVE","i":"1223","x":11,"p":4.08,"s":200,"t":1708453945556,"q":4216525,"z":3},{"ev":"T","sym":"HIVE","i":"842","x":8,"p":4.08,"s":100,"t":1708453945556,"q":4216526,"z":3},{"ev":"T","sym":"BMBL","i":"2784","x":12,"p":13.485,"s":5,"c":[37],"t":1708453945554,"q":4958601,"z":3}]
        ;

    const trades = try std.json.parseFromSlice([]Trade, allocator, msg, .{ .ignore_unknown_fields = true });
    defer trades.deinit();
}
