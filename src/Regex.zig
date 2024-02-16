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

        return .{
            .inner = inner,
        };
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
    start: time.Date,
    end: time.Date,

    const Self = @This();

    pub fn init(pattern: [:0]const u8, start: time.Date, end: time.Date) !Self {
        return Self{
            .regex = try Regex.init(pattern),
            .start = start,
            .end = end,
        };
    }

    pub fn deinit(self: *Self) void {
        self.regex.deinit();
    }

    pub fn matches(self: Self, ticker: [:0]const u8, date: time.Date) bool {
        return date.gt(self.start) and date.lt(self.end) and self.regex.matches(ticker);
    }
};

pub const TickerRegexes = struct {
    ticker_regexes: std.ArrayList(TickerRegex),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, fname: []const u8) !Self {
        const file = try std.fs.cwd().openFile(fname, .{});
        defer file.close();

        var buf_reader = std.io.bufferedReader(file.reader());
        const reader = buf_reader.reader();

        var ticker_regexes = std.ArrayList(TickerRegex).init(allocator);
        errdefer ticker_regexes.deinit();
        var line = std.ArrayList(u8).init(allocator);
        defer line.deinit();

        const writer = line.writer();

        const default_start = time.parseDate("0000-01-01");
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
                const regex = try std.fmt.allocPrintZ(allocator, "^{s}({s})$", .{ line.items, ticker_suffix });
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

        std.log.info("parsed {d} regexes", .{ ticker_regexes.items.len });

        return Self{ .ticker_regexes = ticker_regexes };
    }

    pub fn deinit(self: *Self) void {
        for (self.ticker_regexes.items) |*t| t.deinit();
        self.ticker_regexes.deinit();
    }

    pub fn matches(self: Self, ticker: [:0]const u8, date: time.Date) bool {
        for (self.ticker_regexes.items) |t| {
            if (t.matches(ticker, date)) return true;
        }

        return false;
    }
};

test "basic" {
    const regex = try Regex.init("[ab]c");
    defer regex.deinit();

    try std.testing.expect(regex.matches("bc"));
    try std.testing.expect(!regex.matches("cc"));
}
