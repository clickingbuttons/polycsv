const std = @import("std");
const builtin = @import("builtin");
const clap = @import("clap");
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

fn testTickers(allocator: Allocator) !TickerSet {
    var res = TickerSet.init(allocator);
    const test_tickers = @embedFile("./test_tickers.txt");

    var iter = std.mem.splitScalar(u8, test_tickers, ',');
    while (iter.next()) |t| {
        if (t.len > 0) try res.put(t, {});
    }

    return res;
}

pub fn main() !void {
    const start_default = "2003-09-10";
    const threads_default = 200;
    const max_retries_default: usize = 10;
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                  Display this help and exit.
        \\-s, --start        <str>    Date to start on (YYYY-mm-dd). Inclusive. Defaults to 2003-09-10.
        \\-e, --end          <str>    Date to end on (YYYY-mm-dd). Exclusive. Defaults to when program is run in UTC.
        \\-t, --threads      <u32>    Number of threads. Defaults to 200.
        \\-o, --outdir       <str>    Output directory. Defaults to cwd.
        \\-r, --max-retries  <usize>  Maximum number of times to retry any given HTTP request before exiting. Defaults to 10.
        \\
    );

    const stderr = std.io.getStdErr();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var diag = clap.Diagnostic{};
    var parsed = clap.parse(
        clap.Help,
        &params,
        clap.parsers.default,
        .{ .allocator = allocator, .diagnostic = &diag },
    ) catch |err| {
        diag.report(stderr.writer(), err) catch {};
        return err;
    };
    defer parsed.deinit();

    const args = parsed.args;
    if (args.help != 0) return clap.help(stderr.writer(), clap.Help, &params, .{});

    log_file = try std.fs.cwd().createFile("log.txt", .{});

    var test_tickers = try testTickers(allocator);
    defer test_tickers.deinit();

    const start = try time.Date.parse(args.start orelse start_default);
    // Will cover (start, end)
    // const end = time.Date.now();
    const end = if (args.end) |e| try time.Date.parse(e) else time.Date.now();

    var day = start;
    var date_buf = [_]u8{0} ** 10;
    var n_days: usize = 0;
    while (!std.meta.eql(day, end)) : (day.increment()) {
        if (day.isWeekend()) continue;
        n_days += 1;
    }

    var progress = std.Progress{};
    try start.bufPrint(&date_buf);
    var prog_root = progress.start(&date_buf, n_days);
    prog_root.setUnit(" weekdays");
    progress.refresh();

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{
        .allocator = allocator,
        .n_jobs = args.threads orelse threads_default,
    });
    defer thread_pool.deinit();

    var downloader = try Downloader.init(
        allocator,
        &thread_pool,
        prog_root,
        args.outdir orelse ".",
        args.@"max-retries" orelse max_retries_default,
    );
    defer downloader.deinit();

    day = start;
    while (!std.meta.eql(day, end)) : (day.increment()) {
        if (day.isWeekend()) continue;

        try day.bufPrint(&date_buf);
        prog_root.setName(&date_buf);

        try downloader.download(&date_buf);
        prog_root.completeOne();
    }
    prog_root.end();
}

test {
    _ = @import("./time.zig");
}
