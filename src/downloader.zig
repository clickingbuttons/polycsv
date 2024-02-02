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
                log.err("unexpected grouped daily csv header {s} on {s}", .{ header, day });
                return error.InvalidCsvHeader;
            }
            while (lines.next()) |l| {
                var fields = std.mem.splitScalar(u8, l, ',');
                try res.put(fields.first(), {});
            }
        }

        break :brk res;
    };
    defer tickers.deinit();

    log.info("{s} {d}", .{ day, tickers.unmanaged.size });

    var new_test_tickers = try self.downloadTickers(day, tickers);
    defer new_test_tickers.deinit();

    var iter = new_test_tickers.keyIterator();
    while (iter.next()) |t| log.info("new test ticker {s}", .{t});
}

/// Called owns returned TickerSet
fn downloadTickers(self: *Self, date: []const u8, tickers: TickerSet) !TickerSet {
    const allocator = self.allocator;
    const path = try std.fmt.allocPrint(allocator, "tickers/{s}.csv", .{date});
    defer allocator.free(path);

    var out = try std.fs.cwd().createFile(path, .{});
    defer out.close();

    var prog = self.progress.start("ticker details", tickers.unmanaged.size);
    prog.activate();

    var res = TickerSet.init(allocator);

    const sample = try self.client.tickerDetails("AAPL", "");
    defer allocator.free(sample);

    const newline = std.mem.indexOfScalar(u8, sample, '\n').?;
    const expected_columns = sample[0..newline];
    var column_iter = std.mem.splitScalar(u8, expected_columns, ',');
    var is_test_i: ?usize = null;

    var i: usize = 0;
    while (column_iter.next()) |c| : (i += 1) {
        if (std.mem.eql(u8, c, "is_test")) {
            is_test_i = i;
            break;
        }
    }
    if (is_test_i == null) log.warn("no ticker details test column", .{});

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTickerWorker, .{
            self,
            &out,
            date,
            t.*,
            expected_columns,
            is_test_i,
            &res,
            &prog,
        });
    }
    self.wait_group.wait();

    prog.end();

    return res;
}

fn downloadTickerWorker(
    self: *Self,
    out: *std.fs.File,
    date: []const u8,
    ticker: []const u8,
    expected_header: []const u8,
    is_test_i: ?usize,
    test_tickers: *TickerSet,
    prog: *std.Progress.Node,
) void {
    const allocator = self.allocator;

    defer {
        self.wait_group.finish();
        prog.completeOne();
    }

    var csv = self.client.tickerDetails(ticker, date) catch |err| {
        log.err("error getting details for {s}: {}", .{ ticker, err });
        return;
    };
    defer allocator.free(csv);

    if (csv.len == 0) return; // 404

    var lines = std.mem.splitScalar(u8, csv, '\n');

    if (!std.mem.startsWith(u8, lines.first(), expected_header)) {
        log.err("unexpected header for {s}: {s}", .{ ticker, csv });
        return;
    }

    if (lines.next()) |data| {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (is_test_i) |i| {
            var fields = std.mem.splitScalar(u8, data, ',');
            var test_field: ?[]const u8 = null;
            for (0..i) |_| test_field = fields.next();
            if (test_field) |t| {
                if (std.mem.eql(u8, t, "true")) test_tickers.put(ticker, {}) catch |err| {
                    log.err("{}", .{err});
                    return;
                };
            } else {
                log.err("expected test field at index {d} since header matched", .{i});
            }
        }
        out.writer().writeAll(data) catch |err| {
            log.err("{}", .{err});
            return;
        };
    }
}
