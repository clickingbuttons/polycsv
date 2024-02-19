const std = @import("std");
const argparser = @import("argparser");
const time = @import("./time.zig");
const TickerRegexes = @import("./Regex.zig").TickerRegexes;
const ticker_regexes_description = @import("./main.zig").ticker_regexes_description;

const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;

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
    }, "[file]", null);
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

    for (opt.positional_args.items) |f| try clean(allocator, ticker_regexes, f);
}

const StringSet = struct {
    allocator: std.mem.Allocator,
    set: std.StringHashMapUnmanaged(void) = .{},

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{ .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;
        var iter = self.set.keyIterator();
        while (iter.next()) |k| allocator.free(k.*);
        self.set.deinit(allocator);
    }

    pub fn put(self: *Self, key: []const u8) !void {
        const allocator = self.allocator;
        if (self.set.get(key) == null) {
            const owned = try allocator.dupe(u8, key);
            try self.set.put(allocator, owned, {});
        }
    }
};

fn clean(allocator: Allocator, ticker_regexes: TickerRegexes, fname: []const u8) !void {
    const basename = std.fs.path.basename(fname);
    const date = try time.Date.parse(basename);

    const new_fname = try std.fmt.allocPrint(allocator, "{s}.new", .{fname});
    defer allocator.free(new_fname);

    var in_file = try std.fs.cwd().openFile(fname, .{});
    defer in_file.close();

    var in = gzip.decompressor(in_file.reader());

    var out_file = try std.fs.cwd().createFile(new_fname, .{});
    defer out_file.close();

    var out = try gzip.compressor(out_file.writer(), .{});

    var line = std.ArrayList(u8).init(allocator);
    defer line.deinit();

    var ticker_buf: [32:0]u8 = undefined;
    var line_num: usize = 0;
    var filtered = StringSet.init(allocator);
    defer filtered.deinit();
    var n_lines_filtered: usize = 0;

    while (in.reader().streamUntilDelimiter(line.writer(), '\n', null)) : (line_num += 1) {
        defer line.clearRetainingCapacity();

        if (line_num == 0) {
            try out.writer().writeAll(line.items);
        } else {
            var split = std.mem.splitScalar(u8, line.items, ',');
            const ticker = split.first();

            std.mem.copyForwards(u8, &ticker_buf, ticker);
            ticker_buf[ticker.len + 1] = 0;

            if (ticker_regexes.matches(&ticker_buf, date)) {
                try filtered.put(&ticker_buf);
                n_lines_filtered += 1;
            } else {
                try out.writer().writeAll(line.items);
                try out.writer().writeByte('\n');
            }
        }
    } else |err| switch (err) {
        error.EndOfStream => {},
        else => return err,
    }

    try std.fs.cwd().rename(new_fname, fname);

    std.debug.print("{s} filtered {d} lines for {d} tickers ", .{ fname, n_lines_filtered, filtered.set.size });
    var iter = filtered.set.keyIterator();
    while (iter.next()) |k| std.debug.print("{s} ", .{k.*});
    std.debug.print("\n", .{});
}

test "string set" {
    var s = StringSet.init(std.testing.allocator);
    defer s.deinit();

    try s.put(@as([:0]const u8, "ZVZZT"));
    try s.put(@as([:0]const u8, "ZWZZ"));
    try s.put(@as([:0]const u8, "TESTA"));
    try s.put(@as([:0]const u8, "ZXZZT"));

    try s.put(@as([:0]const u8, "ZXZZT"));
    try s.put(@as([:0]const u8, "TESTA"));
    try s.put(@as([:0]const u8, "ZVZZT"));
    try s.put(@as([:0]const u8, "ZWZZ"));

    var iter = s.set.keyIterator();
    try std.testing.expectEqualStrings("ZVZZT", iter.next().?.*);
    try std.testing.expectEqualStrings("ZWZZ", iter.next().?.*);
    try std.testing.expectEqualStrings("TESTA", iter.next().?.*);
    try std.testing.expectEqualStrings("ZXZZT", iter.next().?.*);
    try std.testing.expectEqual(@as(?*[]const u8, null), iter.next());
}
