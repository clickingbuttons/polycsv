const std = @import("std");
const csv = @import("./csv.zig");
const http = std.http;
const Allocator = std.mem.Allocator;

const Self = @This();
const base = "https://api.polygon.io";

client: http.Client,
headers: std.http.Headers,


pub fn init(allocator: Allocator, apiKey: []const u8) !Self {
    var headers = std.http.Headers{ .allocator = allocator };
    try headers.append("accept", "text/csv");
    const token = try std.fmt.allocPrint(allocator, "Bearer {s}", .{ apiKey });
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

/// Caller owns returned slice.
fn getSlice(self: *Self, comptime T: type, uriString: []const u8) ![]T {
    const allocator = self.client.allocator;

    const uri = try std.Uri.parse(uriString);

    var request = try self.client.request(.GET, uri, self.headers, .{});
    defer request.deinit();

    try request.start();
    try request.wait();

    var reader = csv.reader(T, allocator, request.reader(), .{});
    defer reader.deinit();
    try reader.readHeader();

    var res = std.ArrayList(T).init(allocator);
    errdefer res.deinit();

    while (try reader.next()) |d| try res.append(d);

    return try res.toOwnedSlice();
}

/// Caller owns returned item
fn get(self: *Self, comptime T: type, uriString: []const u8) !?T {
    const allocator = self.client.allocator;

    const uri = try std.Uri.parse(uriString);

    var request = try self.client.request(.GET, uri, self.headers, .{});
    defer request.deinit();

    try request.start();
    try request.wait();

    if (request.response.status != .ok) return error.RateLimit;

    var reader = csv.reader(T, allocator, request.reader(), .{});
    defer reader.deinit();
    try reader.readHeader();

    return try reader.next();
}

pub const GroupedDaily = struct {
    T: []const u8,
    c: f64,
    h: f64,
    l: f64,
    n: u64,
    o: f64,
    t: f64,
    v: f64,
    vw: f64,

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        allocator.free(self.T);
    }
};

pub fn Container(comptime T: type) type {
    return struct {
        items: []T,

        pub fn deinit(self: @This(), allocator: Allocator) void {
            if (@hasDecl(T, "deinit")) {
                for (self.items) |*i| i.deinit(allocator);
            }
            allocator.free(self.items);
        }
    };
}

/// Caller owns returned slice
pub fn grouped_daily(self: *Self, date: []const u8) !Container(GroupedDaily) {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v2/aggs/grouped/locale/us/market/stocks/{s}",
        .{ base, date },
    );
    defer allocator.free(uri);

    const items = try self.getSlice(GroupedDaily, uri);

    return Container(GroupedDaily){ .items = items };
}

pub const TickerDetails = struct {
    ticker: []const u8,
    name: ?[]const u8 = null,
    market: ?[]const u8 = null,
    locale: ?[]const u8 = null,
    primary_exchange: ?[]const u8 = null,
    type: ?[]const u8 = null,
    active: ?bool = null,
    currency_name: ?[]const u8 = null,
    cusip: ?[]const u8 = null,
    cik: ?[]const u8 = null,
    composite_figi: ?[]const u8 = null,
    share_class_figi: ?[]const u8 = null,
    delisted_utc: ?[]const u8 = null,
    phone_number: ?[]const u8 = null,
    address1: ?[]const u8 = null,
    address2: ?[]const u8 = null,
    city: ?[]const u8 = null,
    state: ?[]const u8 = null,
    country: ?[]const u8 = null,
    postal_code: ?[]const u8 = null,
    description: ?[]const u8 = null,
    sic_code: ?[]const u8 = null,
    sic_description: ?[]const u8 = null,
    ticker_root: ?[]const u8 = null,
    ticker_suffix: ?[]const u8 = null,
    homepage_url: ?[]const u8 = null,
    total_employees: ?u32 = null,
    list_date: ?[]const u8 = null,
    logo_url: ?[]const u8 = null,
    icon_url: ?[]const u8 = null,
    accent_color: ?[]const u8 = null,
    light_color: ?[]const u8 = null,
    dark_color: ?[]const u8 = null,
    share_class_shares_outstanding: ?u64 = null,
    weighted_shares_outstanding: ?u64 = null,
    is_test: ?bool = null,
    unit_of_trade: ?u32 = null,
    round_lot: ?u32 = null,

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        inline for (std.meta.fields(@This())) |f| {
            if (comptime f.type == ?[]const u8) {
                if (@field(self, f.name)) |s| allocator.free(s);
            } else if (comptime f.type == []const u8) {
                allocator.free(@field(self, f.name));
            }
        }
    }
};

pub fn tickerDetails(self: *Self, ticker: []const u8, date: []const u8) !?TickerDetails {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/reference/tickers/{s}?date={s}",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.get(TickerDetails, uri);
}
