const std = @import("std");

const http = std.http;
const Allocator = std.mem.Allocator;

const Self = @This();

client: http.Client,
url: []const u8,
uri: std.Uri,

pub fn init(allocator: Allocator, url: []const u8) !Self {
    var client = http.Client{
        .allocator = allocator,
        .next_https_rescan_certs = false,
    };
    errdefer client.deinit();
    try client.ca_bundle.rescan(allocator);

    const next_uri = try allocator.dupe(u8, url);
    const uri = try std.Uri.parse(next_uri);

    return Self{
        .client = client,
        .url = next_uri,
        .uri = uri,
    };
}

pub fn deinit(self: *Self) void {
    self.client.allocator.free(self.url);
    self.client.deinit();
}

pub fn query(self: *Self, q: []const u8) !void {
    const allocator = self.client.allocator;

    const headers = http.Headers{ .allocator = allocator, .owned = false };

    var req = try self.client.open(.POST, self.uri, headers, .{});
    defer req.deinit();

    req.transfer_encoding = .{ .content_length = q.len };
    try req.send(.{});
    try req.writeAll(q);
    try req.finish();
    try req.wait();
}
