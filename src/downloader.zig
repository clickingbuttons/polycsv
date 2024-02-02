const std = @import("std");
const Polygon = @import("./polygon.zig");

pub const TickerSet = std.StringHashMap(void);
const log = std.log;
const Allocator = std.mem.Allocator;
const FileWriter = std.io.BufferedWriter(4096, std.fs.File.Writer).Writer;

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

pub fn download(self: *Self, date: []const u8) !void {
    const allocator = self.allocator;

    const csv = try self.client.groupedDaily(date);
    defer allocator.free(csv);

    var tickers: TickerSet = brk: {
        var res = TickerSet.init(allocator);

        var lines = std.mem.splitScalar(u8, csv, '\n');
        if (lines.next()) |header| {
            if (!std.mem.startsWith(u8, header, "T")) {
                log.err("unexpected grouped daily csv header {s} on {s}", .{ header, date });
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

    log.info("{s} {d}", .{ date, tickers.unmanaged.size });

    var new_test_tickers = try self.downloadTickers(date, tickers);
    defer new_test_tickers.deinit();

    var iter = new_test_tickers.keyIterator();
    while (iter.next()) |t| {
        log.info("new test ticker {s}", .{t.*});
        _ = tickers.remove(t.*);
    }

    try self.downloadTrades(date, tickers);
}

fn columnIndex(columns: []const u8, column: []const u8) ?usize {
    var column_iter = std.mem.splitScalar(u8, columns, ',');
    var i: usize = 0;
    while (column_iter.next()) |c| : (i += 1) {
        if (std.mem.eql(u8, c, column)) return i;
    }

    return null;
}

fn openFile(self: *Self, dir: []const u8, date: []const u8) !std.fs.File {
    const allocator = self.allocator;
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}.csv", .{dir,date});
    defer allocator.free(path);

    try std.fs.cwd().makePath(dir);
    return try std.fs.cwd().createFile(path, .{});
}

/// Caller owns returned TickerSet
fn downloadTickers(self: *Self, date: []const u8, tickers: TickerSet) !TickerSet {
    const allocator = self.allocator;

    var prog = self.progress.start("ticker details", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.openFile("tickers", date);
    defer out.close();

    var buffered = std.io.bufferedWriter(out.writer());
    var writer: FileWriter = buffered.writer();

    var res = TickerSet.init(allocator);

    const sample = try self.client.tickerDetails("AAPL", "");
    defer allocator.free(sample);

    var lines = std.mem.splitScalar(u8, sample, '\n');
    const expected_columns = lines.first();

    const is_test_i = columnIndex(expected_columns, "is_test");
    if (is_test_i == null) log.warn("no ticker details test column", .{});

    try writer.print("{s}\n", .{ expected_columns });

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTickerWorker, .{
            self,
            &writer,
            date,
            t.*,
            expected_columns,
            is_test_i,
            &res,
            &prog,
        });
    }
    self.wait_group.wait();

    try buffered.flush();
    prog.end();
    if (prog.parent) |p| p.setCompletedItems(p.unprotected_completed_items - 1);

    return res;
}

fn downloadTickerWorker(
    self: *Self,
    writer: *FileWriter,
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
        if (data.len == 0) return; // empty response
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
        writer.print("{s}\n", .{ data }) catch |err| {
            log.err("{}", .{err});
            return;
        };
    }
}

/// Caller owns returned TickerSet
fn downloadTrades(self: *Self, date: []const u8, tickers: TickerSet) !void {
    const allocator = self.allocator;

    var prog = self.progress.start("trades", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.openFile("trades", date);
    defer out.close();

    var buffered = std.io.bufferedWriter(out.writer());
    var writer: FileWriter = buffered.writer();

    var sample  = std.ArrayListUnmanaged(u8){};
    defer sample.deinit(allocator);
    try self.client.trades("ASD", "2003-09-10", sample.writer(allocator));

    var lines = std.mem.splitScalar(u8, sample.items, '\n');
    const expected_columns = lines.first();

    try writer.print("ticker,{s}\n", .{ expected_columns });

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTradesWorker, .{
            self,
            &writer,
            date,
            t.*,
            expected_columns,
            &prog,
        });
    }
    self.wait_group.wait();

    try buffered.flush();
    prog.end();
    if (prog.parent) |p| p.setCompletedItems(p.unprotected_completed_items - 1);
}

// The trades endpoint is paginated (requires removing header) AND missing a `ticker` column.
pub const TradesSink = struct {
    header: []const u8,
    out: *FileWriter,
    mutex: *std.Thread.Mutex,
    ticker: []const u8,

    pub fn writeAll(self: @This(), csv: []const u8) !void {
        var lines = std.mem.splitScalar(u8, csv, '\n');
        const header = lines.first();
        if (header.len > 0) {
            if (!std.mem.eql(u8, header, self.header)) {
                log.err("invalid trades header for {s}: {s}", .{ self.ticker, header });
            }
        }

        self.mutex.lock();
        defer self.mutex.unlock();
        while (lines.next()) |l| {
            if (l.len == 0) continue;
            try self.out.print("{s},{s}\n", .{ self.ticker, l });
        }
    }
};

fn downloadTradesWorker(
    self: *Self,
    writer: *FileWriter,
    date: []const u8,
    ticker: []const u8,
    expected_header: []const u8,
    prog: *std.Progress.Node,
) void {
    defer {
        self.wait_group.finish();
        prog.completeOne();
    }

    var sink = TradesSink{
        .header = expected_header,
       .out  = writer,
       .ticker = ticker,
       .mutex = &self.mutex
    };

    self.client.trades(ticker, date, sink) catch |err| {
        log.err("error getting details for {s}: {}", .{ ticker, err });
        return;
    };
}
