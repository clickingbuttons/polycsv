const std = @import("std");

const http = std.http;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const base = "https://api.polygon.io";
const key_var = "POLYGON_KEY";

const Self = @This();

client: http.Client,
token: []const u8,

pub fn init(allocator: Allocator, apiKey: ?[]const u8) !Self {
    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const key = apiKey orelse env.get(key_var) orelse return error.NoApiKey;
    const token = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});

    return Self{
        .client = http.Client{ .allocator = allocator },
        .token = token,
    };
}

pub fn deinit(self: *Self) void {
    self.client.allocator.free(self.token);
    self.client.deinit();
}

fn fetch(self: *Self, uriString: []const u8, accept: []const u8, sink: anytype) !void {
    const allocator = self.client.allocator;
    const uri = try std.Uri.parse(uriString);

    var headers = std.http.Headers{ .allocator = allocator };
    try headers.append("Accept-Encoding", "gzip");
    try headers.append("Authorization", self.token);
    try headers.append("Accept", accept);
    defer headers.deinit();

    const max_tries: usize = 5;
    var n_tries: usize = 0;
    while (n_tries < max_tries) : ({
        n_tries += 1;
        std.time.sleep(n_tries * n_tries * std.time.ns_per_s);
    }) {
        var request = self.client.open(.GET, uri, headers, .{}) catch |err| {
            std.log.warn("try {d}/{d} requesting {s}: {}", .{ n_tries, max_tries, uriString, err });
            continue;
        };
        defer request.deinit();

        request.send(.{}) catch |err| {
            std.log.warn("try {d}/{d} starting {s}: {}", .{ n_tries, max_tries, uriString, err });
            continue;
        };
        request.wait() catch |err| {
            std.log.warn("try {d}/{d} waiting {s}: {}", .{ n_tries, max_tries, uriString, err });
            continue;
        };

        switch (request.response.status) {
            .ok => {
                //{
                //    var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
                //    defer fifo.deinit();
                //    try fifo.pump(request.reader(), sink);
                //}

                // instead of efficiently piping the request (which makes for hard parsing that has to lock)
                // just send a whole response to parse
                {
                    const data = try request.reader().readAllAlloc(allocator, 1 << 32);
                    defer allocator.free(data);
                    try sink.writeAll(data);
                }

                if (request.response.headers.getFirstValue("link")) |l| {
                    if (std.mem.indexOfScalar(u8, l, '>')) |end| {
                        if (end == 0) return;
                        const next_url = l[1..end];
                        try self.fetch(next_url, accept, sink);
                    }
                }
                return;
            },
            .not_found => return,
            else => {
                std.log.warn("bad response from {s}: {}", .{ uriString, request.response.status });
                return error.BadResponse;
            },
        }
    }
}

/// Caller owns returned slice
fn getAlloc(self: *Self, uriString: []const u8, accept: []const u8) ![]const u8 {
    const allocator = self.client.allocator;
    var buf = std.ArrayListUnmanaged(u8){};

    try self.fetch(uriString, accept, buf.writer(allocator));

    return try buf.toOwnedSlice(allocator);
}

pub fn Parsed(comptime T: type) type {
    return struct {
        arena: *ArenaAllocator,
        value: ?T = null,
        json: []const u8,

        pub fn deinit(self: @This()) void {
            const allocator = self.arena.child_allocator;
            self.arena.deinit();
            allocator.free(self.json);
            allocator.destroy(self.arena);
        }
    };
}

fn fetchJson(self: *Self, comptime T: type, uri: []const u8) !Parsed(T) {
    const allocator = self.client.allocator;

    const json = try self.getAlloc(uri, "application/json");
    errdefer allocator.free(json);

    if (json.len == 0) {
        const res = Parsed(T){
            .arena = try allocator.create(ArenaAllocator),
            .json = json,
        };
        res.arena.* = ArenaAllocator.init(allocator);
        return res;
    }

    const json_parsed = try std.json.parseFromSlice(
        struct { results: ?T = null },
        allocator,
        json,
        .{ .ignore_unknown_fields = true },
    );

    return Parsed(T){
        .arena = json_parsed.arena,
        .value = json_parsed.value.results,
        .json = json,
    };
}

/// Caller owns returned slice
pub fn groupedDaily(self: *Self, date: []const u8) !Parsed(GroupedDaily) {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v2/aggs/grouped/locale/us/market/stocks/{s}?include_otc=false",
        .{ base, date },
    );
    defer allocator.free(uri);

    return try self.fetchJson(GroupedDaily, uri);
}

/// Caller owns returned slice
pub fn tickerDetails(self: *Self, ticker: []const u8, date: []const u8) !Parsed(TickerDetails) {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/reference/tickers/{s}?date={s}",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.fetchJson(TickerDetails, uri);
}

pub fn trades(self: *Self, ticker: []const u8, date: []const u8, sink: anytype) !void {
    const allocator = self.client.allocator;

    const uri = try std.fmt.allocPrint(
        allocator,
        "{s}/v3/trades/{s}?timestamp={s}&limit=50000",
        .{ base, ticker, date },
    );
    defer allocator.free(uri);

    return try self.fetch(uri, "text/csv", sink);
}

pub const GroupedDaily = []struct { T: []const u8 };

pub const TickerDetails = struct {
    ticker: []const u8,
    name: []const u8,
    active: bool,
    primary_exchange: []const u8 = "",
    type: []const u8 = "",
    is_test: bool = false,
    cik: []const u8 = "",
    composite_figi: []const u8 = "",
    share_class_figi: []const u8 = "",
    phone_number: []const u8 = "",
    description: []const u8 = "",
    sic_code: []const u8 = "",
    sic_description: []const u8 = "",
    ticker_root: []const u8 = "",
    homepage_url: []const u8 = "",
    total_employees: ?usize = null,
    list_date: []const u8 = "",
    share_class_shares_outstanding: ?usize = null,
    weighted_shares_outstanding: ?usize = null,
    round_lot: ?usize = null,
};
