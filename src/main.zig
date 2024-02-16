const std = @import("std");
const builtin = @import("builtin");
const argparser = @import("argparser");
const Polygon = @import("./polygon.zig");
const time = @import("./time.zig");
const Downloader = @import("./downloader.zig");

const Allocator = std.mem.Allocator;
const http = std.http;
const TickerSet = Downloader.TickerSet;

var log_file: std.fs.File = undefined;
var log_mutex = std.Thread.Mutex{};

pub const std_options = struct {
    pub const logFn = myLogFn;

    pub const log_level: std.log.Level = switch (builtin.mode) {
        .Debug => .debug,
        else => .info,
    };
};

pub fn myLogFn(
    comptime message_level: std.log.Level,
    comptime scope: @Type(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    _ = scope;
    log_mutex.lock();
    defer log_mutex.unlock();
    var writer = log_file.writer();

    const secs = @divTrunc(std.time.milliTimestamp(), 1000);
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @intCast(secs) };

    writer.writeByte('[') catch return;

    const epoch_day = epoch_seconds.getEpochDay();
    const epoch_year = epoch_day.calculateYearDay();
    const epoch_month = epoch_year.calculateMonthDay();
    writer.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ epoch_year.year, epoch_month.month.numeric(), epoch_month.day_index + 1 }) catch return;

    const epoch_time = epoch_seconds.getDaySeconds();
    const hour = epoch_time.getHoursIntoDay();
    const minute = epoch_time.getMinutesIntoHour();
    const second = epoch_time.getSecondsIntoMinute();
    writer.print("T{d:0>2}:{d:0>2}:{d:0>2}Z", .{ hour, minute, second }) catch return;

    writer.writeByte(']') catch return;

    writer.writeAll("[" ++ comptime message_level.asText() ++ "] ") catch return;
    writer.print(format, args) catch return;
    writer.writeByte('\n') catch return;
}

fn panic(comptime format: []const u8, args: anytype) void {
    std.log.err(format, args);
    std.process.exit(1);
    noreturn;
}

pub fn main() !void {
    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var opt = try argparser.parse(allocator, struct {
        help: bool = false,
        @"help-test-tickers": bool = false,
        start: []const u8 = "2003-09-10",
        end: ?[]const u8,
        threads: u32 = 300,
        outdir: []const u8 = ".",
        @"max-retries": usize = 60,
        @"skip-trades": bool = false,
        @"test-tickers": []const u8 = "test_tickers.txt",
        @"log-file": []const u8 = "log.txt",

        pub const __shorts__ = .{
            .start = .s,
            .end = .e,
            .threads = .t,
            .outdir = .o,
            .@"max-retries" = .r,
        };

        pub const __messages__ = .{
            .end = "Defaults to today in UTC.",
            .@"test-tickers" = "See help-test-tickers. ",
        };
    }, null, null);
    defer opt.deinit();

    const args = opt.args;

    if (args.help) {
        try opt.print_help(stderr.writer());
        return;
    } else if (args.@"help-test-tickers") {
        try stderr.writer().writeAll(
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
        );
        return;
    }

    log_file = try std.fs.cwd().createFile(args.@"log-file", .{});

    const start = try time.Date.parse(args.start);
    const end = if (args.end) |e| try time.Date.parse(e) else time.Date.now();

    var date_buf = [_]u8{0} ** 10;
    var iter = time.DateIterator.init(start, end, true);

    var n_days: usize = 0;
    while (iter.next()) |_| n_days += 1;
    iter.reset();

    var progress = std.Progress{};
    try start.bufPrint(&date_buf);
    var prog_root = progress.start(&date_buf, n_days);
    prog_root.setUnit(" weekdays");
    progress.refresh();

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{ .allocator = allocator, .n_jobs = args.threads });
    defer thread_pool.deinit();

    var downloader = try Downloader.init(
        allocator,
        &thread_pool,
        prog_root,
        args.outdir,
        args.@"max-retries",
        args.@"skip-trades",
        args.@"test-tickers",
    );
    defer downloader.deinit();

    while (iter.next()) |day| {
        try day.bufPrint(&date_buf);
        prog_root.setName(&date_buf);

        try downloader.download(&date_buf);
        prog_root.completeOne();
    }
    prog_root.end();
}

test {
    _ = @import("./time.zig");
    _ = @import("./Regex.zig");
}
