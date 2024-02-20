const std = @import("std");
const argparser = @import("argparser");
const TickerRegexes = @import("./Regex.zig").TickerRegexes;
const websocket = @import("websocket");
const key_var = @import("./polygon.zig").key_var;

const log = std.log;
const Allocator = std.mem.Allocator;

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var opt = try argparser.parse(allocator, struct {
        help: bool = false,
        @"help-test-tickers": bool = false,
        @"test-tickers": []const u8 = "test_tickers.txt",

        pub const __messages__ = .{
            .@"test-tickers" = "See help-test-tickers. ",
        };
    }, "", null);
    defer opt.deinit();

    const args = opt.args;

    if (args.help) {
        try opt.print_help(stderr.writer());
        return;
    } else if (args.@"help-test-tickers") {
        try stderr.writer().writeAll(TickerRegexes.description);
        return;
    }

    var ticker_regexes = try TickerRegexes.init(allocator, args.@"test-tickers");
    defer ticker_regexes.deinit();

    var handler = try Handler.init(allocator, null, "XT.*");
    defer handler.deinit();

    try handler.client.readLoop(&handler);
}

fn get_path(channel: []const u8) []const u8 {
    if (std.mem.startsWith(u8, channel, "XT.")) return "/crypto";

    return "/stocks";
}

const Handler = struct {
    const host = "socket.polygon.io";
    const port = 443;
    const Self = @This();

    client: websocket.Client,
    ca_bundle: std.crypto.Certificate.Bundle,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, api_key: ?[]const u8, channel: []const u8) !Handler {
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
        };
    }

    pub fn deinit(self: *Self) void {
        self.ca_bundle.deinit(self.allocator);
        self.client.deinit();
    }

    pub fn handle(_: Self, message: websocket.Message) !void {
        const data = message.data;
        std.debug.print("msg {s}\n", .{data});
    }

    pub fn close(_: Self) void {}
};
