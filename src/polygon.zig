const std = @import("std");

const http = std.http;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const base = "https://api.polygon.io";
pub const key_var = "POLYGON_KEY";

const Self = @This();

client: http.Client,
token: []const u8,
max_retries: usize,

pub fn init(allocator: Allocator, max_retries: usize, n_threads: usize, apiKey: ?[]const u8) !Self {
    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const key = apiKey orelse env.get(key_var) orelse return error.NoApiKey;
    const token = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});

    var client = http.Client{
        .allocator = allocator,
        .connection_pool = .{ .free_size = n_threads },
        .next_https_rescan_certs = false,
    };
    try client.ca_bundle.rescan(allocator);

    return Self{
        .client = client,
        .token = token,
        .max_retries = max_retries,
    };
}

pub fn deinit(self: *Self) void {
    self.client.allocator.free(self.token);
    self.client.deinit();
}

fn fetchInner(self: *Self, next_uri: []const u8, accept: []const u8, sink: anytype) !?[]const u8 {
    const allocator = self.client.allocator;
    const uri = try std.Uri.parse(next_uri);

    var headers = std.http.Headers{ .allocator = allocator };
    try headers.append("Authorization", self.token);
    try headers.append("Accept", accept);
    defer headers.deinit();

    const max_tries = self.max_retries;
    var n_tries: usize = 0;
    var retry_immediately = false;

    while (n_tries <= max_tries) : ({
        const sleep_s: usize = if (retry_immediately) 0 else @min(n_tries * n_tries, 60);
        if (retry_immediately) {
            retry_immediately = false;
        } else {
            n_tries += 1;
        }
        std.time.sleep(sleep_s * std.time.ns_per_s);
    }) {
        var request = self.client.open(.GET, uri, headers, .{}) catch |err| {
            std.log.warn("open {d}/{d} {s}: {}", .{ n_tries, max_tries, next_uri, err });
            continue;
        };
        defer request.deinit();

        request.send(.{}) catch |err| {
            std.log.warn("send {d}/{d} {s}: {}", .{ n_tries, max_tries, next_uri, err });
            continue;
        };
        request.wait() catch |err| {
            std.log.warn("wait {d}/{d} {s}: {}", .{ n_tries, max_tries, next_uri, err });
            switch (err) {
                // From the std.http.Client author:
                // > EndOfStream means the peer closed the connection, and std.http kept trying to
                // > read from it (primarily because the only way to know this is try to read from it);
                // > std.http doesn't handle this case very well at the moment,
                // > as it currently expects the server to hold a connection open forever
                // > (which is generally a safe assumption in a single-shot or burst of requests).
                // > Assuming you're dealing with non-malicious servers, it should be reasonably safe
                // to treat any EndOfStream as an ignorable error and not count it towards a retry limit
                error.EndOfStream => retry_immediately = true,
                // I have experienced `request.reader()`s hanging when other threads when seeing this.
                // As a crude way to prevent this, reset all connections.
                error.ConnectionResetByPeer => {
                    const old_size = self.client.connection_pool.free_size;
                    self.client.connection_pool.resize(allocator, 0);
                    self.client.connection_pool.resize(allocator, old_size);
                },
                else => {},
            }
            continue;
        };

        switch (@intFromEnum(request.response.status)) {
            200 => {
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
                        if (end == 0) return null;
                        return try allocator.dupe(u8, l[1..end]);
                    }
                }
                return null;
            },
            400...500 => |c| {
                if (c == 404) return null;

                std.log.err("{} from '{s}'", .{ request.response.status, next_uri });
                const data = try request.reader().readAllAlloc(allocator, 1 << 32);
                defer allocator.free(data);
                std.log.err("body {s}", .{data});
                @panic("can't be making bad requests");
            },
            else => {
                std.log.warn("{} from '{s}'", .{ request.response.status, next_uri });
                continue;
            },
        }
    }

    return null;
}

fn fetch(self: *Self, uri: []const u8, accept: []const u8, sink: anytype) !void {
    const allocator = self.client.allocator;

    var next_uri = try self.fetchInner(uri, accept, sink);
    while (next_uri) |u| {
        next_uri = try self.fetchInner(u, accept, sink);
        allocator.free(u);
    }
}

/// Caller owns returned slice
fn getAlloc(self: *Self, uri: []const u8, accept: []const u8) ![]const u8 {
    const allocator = self.client.allocator;
    var buf = std.ArrayListUnmanaged(u8){};

    try self.fetch(uri, accept, buf.writer(allocator));

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

pub const GroupedDaily = []struct { T: [:0]const u8 };

pub const TickerDetails = struct {
    ticker: []const u8,
    active: bool,
    name: []const u8 = "",
    primary_exchange: []const u8 = "",
    type: []const u8 = "",
    is_test: bool = false,
    cik: []const u8 = "",
    composite_figi: []const u8 = "",
    share_class_figi: []const u8 = "",
    phone_number: []const u8 = "",
    address: struct {
        address1: []const u8 = "",
        address2: []const u8 = "",
        city: []const u8 = "",
        state: []const u8 = "",
        postal_code: []const u8 = "",
    } = .{},
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
