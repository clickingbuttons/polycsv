const std = @import("std");
const c = @cImport({
    @cInclude("regex_slim.h");
});
const time = @import("./time.zig");

// Polygon uses the CQS symbol convention.
// https://nasdaqtrader.com/trader.aspx?id=CQSsymbolconvention
const ticker_suffix = "p[A-Z]?|(\\.WS)?\\.[A-Z]|p?\\.WD|\\.[A-Z]|p?\\.[A-Z]?CL|p[A-Z]w|\\.EC|\\.PP||\\.CV||\\.[A-Z]CV|p[A-Z]\\.(CV|WD)|r|\\.U|r?p?w|\\.Aw|\\.WSw";

pub const Regex = struct {
    inner: *c.regex_t,

    const Self = @This();

    fn init(pattern: [:0]const u8) !Self {
        const inner = c.alloc_regex_t().?;
        if (0 != c.regcomp(inner, pattern, c.REG_NEWLINE | c.REG_EXTENDED)) {
            return error.compile;
        }

        return Self{ .inner = inner };
    }

    fn deinit(self: Self) void {
        c.free_regex_t(self.inner);
    }

    fn matches(self: Self, input: [:0]const u8) bool {
        const match_size = 1;
        var pmatch: [match_size]c.regmatch_t = undefined;
        return 0 == c.regexec(self.inner, input, match_size, &pmatch, 0);
    }
};

const TickerRegex = struct {
    regex: Regex,
    start: u64,
    end: u64,

    const Self = @This();

    pub fn init(pattern: [:0]const u8, start: time.Date, end: time.Date) !Self {
        return Self{
            .regex = try Regex.init(pattern),
            .start = start.epochSeconds(),
            .end = end.epochSeconds(),
        };
    }

    pub fn deinit(self: *Self) void {
        self.regex.deinit();
    }

    pub fn matches(self: Self, ticker: [:0]const u8, date: time.Date) bool {
        const secs = date.epochSeconds();
        return secs > self.start and secs < self.end and self.regex.matches(ticker);
    }
};

pub const TickerRegexes = struct {
    ticker_regexes: std.ArrayList(TickerRegex),
    ticker_buf: [32]u8 = undefined,

    pub const description =
        \\Polygon does not authoritatively define test tickers.
        \\Exchanges publish lists and retain the rights to create new ones.
        \\In order to not download these you can modify or make your own `test_tickers.txt`.
        \\
        \\It's a file with list of regexes, each on a newline.
        \\Each has `^` prepended and `(CQS suffixes)?$` appended.
        \\Comments start with `;`.
        \\Kvs for the following line start with `;!` and have syntax `key=value`.
        \\Supported optional kvs:
        \\  - `start`: The date which this regex starts identifying a test ticker.
        \\  - `end`: The date which this regex ends identifying a test ticker.
        \\
    ;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, fname: []const u8) !Self {
        var ticker_regexes = std.ArrayList(TickerRegex).init(allocator);
        errdefer ticker_regexes.deinit();

        const file = std.fs.cwd().openFile(fname, .{}) catch |err| {
            std.log.warn("not using ticker regexes in {s}: {}", .{ fname, err });
            return Self{ .ticker_regexes = ticker_regexes };
        };
        defer file.close();

        var buf_reader = std.io.bufferedReader(file.reader());
        const reader = buf_reader.reader();

        var line = std.ArrayList(u8).init(allocator);
        defer line.deinit();
        const writer = line.writer();

        const default_start = time.parseDate("1970-01-01");
        const default_end = time.parseDate("3000-01-01");
        var start = default_start;
        var end = default_end;

        while (reader.streamUntilDelimiter(writer, '\n', null)) {
            defer line.clearRetainingCapacity();

            if (std.mem.startsWith(u8, line.items, ";")) {
                if (std.mem.startsWith(u8, line.items, ";!")) {
                    var kvs = std.mem.splitScalar(u8, line.items[2..], ' ');
                    while (kvs.next()) |kv_str| {
                        var kv = std.mem.splitScalar(u8, kv_str, '=');
                        const key = kv.first();
                        const value = kv.rest();
                        if (std.mem.eql(u8, key, "start")) {
                            start = try time.Date.parse(value);
                        } else if (std.mem.eql(u8, key, "end")) {
                            end = try time.Date.parse(value);
                        }
                    }
                }
            } else if (line.items.len > 0) {
                const regex = try std.fmt.allocPrintZ(allocator, "^{s}({s})?$", .{ line.items, ticker_suffix });
                defer allocator.free(regex);
                const ticker_regex = try TickerRegex.init(regex, start, end);
                try ticker_regexes.append(ticker_regex);
                start = default_start;
                end = default_end;
            }
        } else |err| switch (err) {
            error.EndOfStream => {},
            else => return err,
        }

        std.log.info("parsed {d} regexes in {s}", .{ ticker_regexes.items.len, fname });

        return Self{ .ticker_regexes = ticker_regexes };
    }

    pub fn deinit(self: *Self) void {
        for (self.ticker_regexes.items) |*t| t.deinit();
        self.ticker_regexes.deinit();
    }

    pub fn matches(self: *Self, ticker: []const u8, date: time.Date) bool {
        std.mem.copyForwards(u8, &self.ticker_buf, ticker);
        self.ticker_buf[ticker.len] = 0;
        const ticker_z: [:0]const u8 = @ptrCast(self.ticker_buf[0..ticker.len]);

        for (self.ticker_regexes.items) |t| {
            if (t.matches(ticker_z, date)) return true;
        }

        return false;
    }
};

test "regex basic" {
    const regex = try Regex.init("[ab]c");
    defer regex.deinit();

    try std.testing.expect(regex.matches("bc"));
    try std.testing.expect(!regex.matches("cc"));
}

test "regex tickers" {
    const allocator = std.testing.allocator;
    var regexes = try TickerRegexes.init(allocator, "test_tickers.txt");
    defer regexes.deinit();

    try std.testing.expect(regexes.matches("ZTST", time.parseDate("2010-09-10")));
    try std.testing.expect(!regexes.matches("ZTS", time.parseDate("2010-09-10")));
    try std.testing.expect(regexes.matches("CBO", time.parseDate("2020-09-10")));
    try std.testing.expect(!regexes.matches("CBO", time.parseDate("2003-09-10")));
    try std.testing.expect(!regexes.matches("CB", time.parseDate("2003-09-10")));
    try std.testing.expect(regexes.matches("ZZZ", time.parseDate("2010-12-17")));
    try std.testing.expect(!regexes.matches("ZZ", time.parseDate("2010-12-17")));
    try std.testing.expect(!regexes.matches("ZZZ", time.parseDate("2023-12-17")));
}
