const std = @import("std");
const argparser = @import("argparser");
const TickerRegexes = @import("./Regex.zig").TickerRegexes;
const websocket = @import("websocket");
const key_var = @import("./polygon.zig").key_var;
const csv_mod = @import("./CsvWriter.zig");
const Clickhouse = @import("./clickhouse.zig");

const debug = std.log.debug;
const Allocator = std.mem.Allocator;
const State = enum { running, flushing };

var state: State = .running;

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var opt = try argparser.parse(allocator, struct {
        help: bool = false,
        @"help-test-tickers": bool = false,
        @"test-tickers": []const u8 = "test_tickers.txt",
        output: []const u8 = "out.csv",
        uri: []const u8 = "wss://socket.polygon.io",

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

    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const key = env.get("POLYGON_KEY") orelse return error.NoApiKey;

    var ticker_regexes = try TickerRegexes.init(allocator, args.@"test-tickers");
    defer ticker_regexes.deinit();

    const topics = try std.mem.join(allocator, ",", opt.positional_args.items);
    defer allocator.free(topics);

    const market = Market.fromChannel(opt.positional_args.items[0]);
    const T = switch (market) {
        .crypto => CryptoTrade,
        .stocks => Trade,
    };

    var uri = try std.Uri.parse(args.uri);
    uri.path = switch (market) {
        .crypto => "/crypto",
        .stocks => "/stocks",
    };

    var out_file = try std.fs.cwd().createFile(args.output, .{});
    defer out_file.close();

    var file_writer = std.io.bufferedWriter(out_file.writer());
    var csv_writer = csv_mod.csvWriter(T, file_writer.writer());

    var clickhouse = try Clickhouse.init(allocator, "http://localhost:8123/?async_insert=1&wait_for_async_insert=0");
    defer clickhouse.deinit();

    var query_buf = std.ArrayList(u8).init(allocator);
    defer query_buf.deinit();

    try std.os.sigaction(std.os.SIG.INT, &.{
        .handler = .{
            .handler = struct {
                fn func(sig: c_int) callconv(.C) void {
                    _ = sig;
                    state = .flushing;
                }
            }.func,
        },
        .mask = std.os.empty_sigset,
        .flags = 0,
    }, null);

    {
        var ws = try websocket.connect(allocator, uri, .{});
        defer ws.deinit(allocator);

        var send_buffer: [4096]u8 = undefined;

        const auth_msg  = try ws.receive();
        debug("welcome {s}", .{auth_msg.data});

        var to_send = try std.fmt.bufPrint(&send_buffer, "{{\"action\": \"auth\", \"params\": \"{s}\"}}", .{ key });
        try ws.send(.text, to_send);

        const authed_msg = try ws.receive();
        debug("authed {s}", .{authed_msg.data});

        to_send = try std.fmt.bufPrint(&send_buffer, "{{\"action\": \"subscribe\", \"params\": \"{s}\"}}", .{ topics });
        try ws.send(.text, to_send);

        const subbed_msg = try ws.receive();
        debug("subbed {s}", .{subbed_msg.data});

        while (state == .running) {
            const msg = try ws.receive();

            switch (msg.type) {
                .text => {
                    const data = msg.data;
                    debug("{d}", .{ data.len});

                    const trades = std.json.parseFromSlice([]T, allocator, data, .{ .ignore_unknown_fields = true }) catch |err| {
                        std.debug.print("could not parse ", .{});
                        for (data, 0..) |c, i| {
                            if (i > 100) break;
                            std.debug.print("{d} ", .{ c });
                            std.debug.print("{s} ", .{ data });
                        }
                        std.debug.print("\n", .{});
                        return err;
                    };

                    query_buf.clearRetainingCapacity();
                    var writer = query_buf.writer();
                    try writer.writeAll("insert into us_equities.trades values ");
                    for (trades.value) |t| {
                        try csv_writer.writeRecord(t);
                        //try writer.print("({d}, {d}, {s}, '{s}', fromUnixTimestamp64Milli({d}), 0, fromUnixTimestamp64Milli({d}), {d}, {d}, [", .{
                        //    t.q,
                        //    t.z,
                        //    t.i,
                        //    t.sym,
                        //    t.t,
                        //    t.trft,
                        //    t.p,
                        //    t.s,
                        //});
                        //for (t.c, 0..) |c, i| {
                        //    try writer.print("{d}", .{c});
                        //    if (i != t.c.len - 1) try writer.writeByte(',');
                        //}
                        //try writer.print("], 0, {d}, {d}), ", .{
                        //    t.x,
                        //    t.trfi,
                        //});
                    }
                    // const t = std.time.milliTimestamp();
                    // try clickhouse.query(self.query_buf.items);
                    // std.debug.print("total {d}\n", .{std.time.milliTimestamp() - t});
                    defer trades.deinit();
                },

                .ping => {
                    debug("got ping! sending pong...", .{});
                    try ws.pong();
                },

                .close => {
                    debug("close", .{});
                    break;
                },

                else => {
                    debug("got {s}: {s}", .{@tagName(msg.type), msg.data});
                },
            }
        }

        try file_writer.flush();
        try ws.close();
        debug("done", .{});
    }
}

const Market = enum {
    crypto,
    stocks,

    pub fn fromChannel(channel: []const u8) @This() {
        if (std.mem.startsWith(u8, channel, "X")) return .crypto;

        return .stocks;
    }
};

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
    trfi: u8 = 0,
    // trf timestamp
    trft: usize = 0,
};

pub const CryptoTrade = struct {
    pair: []const u8,
    p: f64,
    t: u64,
    s: f64,
    c: []u8 = &[_]u8{},
    i: []const u8,
    x: u8,
    r: u64,
};
