const std = @import("std");
const Polygon = @import("./polygon.zig");

pub const TickerSet = std.StringHashMap(void);
const log = std.log;
const Allocator = std.mem.Allocator;

allocator: Allocator,
client: Polygon,
progress: *std.Progress.Node,
thread_pool: *std.Thread.Pool,
wait_group: std.Thread.WaitGroup = .{},
mutex: std.Thread.Mutex = .{},

const Self = @This();

pub fn init(allocator: Allocator, thread_pool: *std.Thread.Pool, progress: *std.Progress.Node) !Self {
    const client = try Polygon.init(allocator, null);

    return Self{
        .allocator = allocator,
        .client = client,
        .progress = progress,
        .thread_pool = thread_pool,
    };
}

pub fn deinit(self: *Self) void {
    self.client.deinit();
}

pub fn download(self: *Self, day: []const u8) !void {
    const allocator = self.allocator;

    const csv = try self.client.groupedDaily(day);
    defer allocator.free(csv);

    var tickers: TickerSet = brk: {
        var res = TickerSet.init(allocator);

        var lines = std.mem.splitScalar(u8, csv, '\n');
        if (lines.next()) |header| {
            if (!std.mem.startsWith(u8, header, "T")) {
                log.err("grouped daily csv header {s}", .{ header });
                return error.InvalidCsvHeader;
            }
            var i: usize = 0;
            while (lines.next()) |l| : (i += 1) {
                var fields = std.mem.splitScalar(u8, l, ',');
                if (fields.next()) |first| {
                    try res.put(first, {});
                }
                if (i == 0) break;
            }
        }

        break :brk res;
    };
    defer tickers.deinit();

    log.info("{s} {d}", .{ day, tickers.unmanaged.size });

    try self.downloadTickers(day, tickers);
}

fn downloadTickers(self: *Self, date: []const u8, tickers: TickerSet)  !void {
    const allocator = self.allocator;
    const path = try std.fmt.allocPrint(allocator, "tickers/{s}.csv", .{ date });
    defer allocator.free(path);

    var out = try std.fs.cwd().createFile(path, .{});
    defer out.close();

    var prog = self.progress.start("ticker details", tickers.unmanaged.size);
    prog.activate();

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTickerWorker, .{
            self,
            &out,
            date,
            t.*,
            &prog,
        });
    }
    self.wait_group.wait();

   prog.end();
}

fn downloadTickerWorker(
    self: *Self,
    out: *std.fs.File,
    date: []const u8,
    ticker: []const u8,
    prog: *std.Progress.Node,
) void {
    const allocator = self.allocator;

    defer {
        self.wait_group.finish();
        prog.completeOne();
    }

    var details = self.client.tickerDetails(ticker, date) catch |err| {
        log.err("{}", .{ err });
        return;
    };
    defer allocator.free(details);

    if (std.mem.indexOfScalar(u8, details, '\n')) |first_newline| {
        self.mutex.lock();
        defer self.mutex.unlock();
        out.writer().writeAll(details[first_newline + 1..]) catch |err| {
            log.err("{}", .{ err });
            return;
        };
    }
}
