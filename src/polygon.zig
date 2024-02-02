const std = @import("std");
const http = std.http;
const Allocator = std.mem.Allocator;

const base = "https://api.polygon.io";
const key_var = "POLYGON_KEY";

const Self = @This();

client: http.Client,
headers: std.http.Headers,

pub fn init(allocator: Allocator, apiKey: ?[]const u8) !Self {
    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const key = apiKey orelse env.get(key_var) orelse return error.NoApiKey;

    var headers = std.http.Headers{ .allocator = allocator };
    try headers.append("accept", "text/csv");
    const token = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});
    defer allocator.free(token);
    try headers.append("authorization", token);

    return Self{
        .client = http.Client{ .allocator = allocator },
        .headers = headers,
    };
}

pub fn deinit(self: *Self) void {
    self.headers.deinit();
    self.client.deinit();
}

fn get(self: *Self, uriString: []const u8, sink: anytype) !void {
    const allocator = self.client.allocator;
    const uri = try std.Uri.parse(uriString);

    var request = try self.client.request(.GET, uri, self.headers, .{});
    defer request.deinit();

    try request.start();
    try request.wait();

    if (request.response.status == .not_found) return;
    if (request.response.status != .ok) return error.RateLimit;

    //{
    //    var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
    //    defer fifo.deinit();
    //    try fifo.pump(request.reader(), sink);
    //}

    // instead of efficiently piping the request (which makes for hard parsing that has to lock)
    // just send a whole CSV at a time
    {
        const csv = try request.reader().readAllAlloc(allocator, 1 << 32);
        defer allocator.free(csv);
        try sink.writeAll(csv);
    }

    if (request.response.headers.getFirstValue("link")) |l| {
        if (std.mem.indexOfScalar(u8, l, '>')) |end| {
            if (end == 0) return;
            const next_url = l[1..end];
            try self.get(next_url, sink);
        }
    }
}

/// Caller owns returned slice
fn getAlloc(self: *Self, uriString: []const u8) ![]const u8 {
    const allocator = self.client.allocator;
    var buf  = std.ArrayListUnmanaged(u8){};

    try self.get(uriString, buf.writer(allocator));

    return try buf.toOwnedSlice(allocator);
}

/// Caller owns returned slice
pub fn groupedDaily(self: *Self, date: []const u8) ![]const u8 {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v2/aggs/grouped/locale/us/market/stocks/{s}",
        .{ base, date },
    );
    defer allocator.free(uri);

    return try self.getAlloc(uri);
}

/// Caller owns returned slice
pub fn tickerDetails(self: *Self, ticker: []const u8, date: []const u8) ![]const u8 {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/reference/tickers/{s}?date={s}",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.getAlloc(uri);
}

pub fn trades(self: *Self, ticker: []const u8, date: []const u8, sink: anytype) !void {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/trades/{s}?timestamp={s}&limit=50000",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.get(uri, sink);
}
