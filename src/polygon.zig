const std = @import("std");
const csv = @import("./csv.zig");
const http = std.http;
const Allocator = std.mem.Allocator;

const Self = @This();
const base = "https://api.polygon.io";
const key_var = "POLYGON_KEY";

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

/// Caller owns returned item
fn get(self: *Self, uriString: []const u8) ![]const u8 {
    const allocator = self.client.allocator;

    const uri = try std.Uri.parse(uriString);

    var request = try self.client.request(.GET, uri, self.headers, .{});
    defer request.deinit();

    try request.start();
    try request.wait();

    if (request.response.status == .not_found) return try allocator.alloc(u8, 0);
    if (request.response.status != .ok) return error.RateLimit;

    return try request.reader().readAllAlloc(allocator, 1 << 32);
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

    return try self.get(uri);
}

pub fn tickerDetails(self: *Self, ticker: []const u8, date: []const u8) ![]const u8 {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/reference/tickers/{s}?date={s}",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.get(uri);
}
