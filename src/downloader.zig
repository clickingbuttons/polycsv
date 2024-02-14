const std = @import("std");
const Polygon = @import("./polygon.zig");
const csv_mod = @import("./CsvWriter.zig");

pub const TickerSet = std.StringHashMap(void);
const log = std.log;
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const FileWriter = gzip.Compress(std.fs.File.Writer).Writer;
const TickerDetails = Polygon.TickerDetails;

allocator: Allocator,
client: Polygon,
progress: *std.Progress.Node,
thread_pool: *std.Thread.Pool,
wait_group: std.Thread.WaitGroup = .{},
mutex: std.Thread.Mutex = .{},
outdir: []const u8,
skip_trades: bool,

const Self = @This();

pub fn init(
    allocator: Allocator,
    thread_pool: *std.Thread.Pool,
    progress: *std.Progress.Node,
    outdir: []const u8,
    max_retries: usize,
    skip_trades: bool,
) !Self {
    const client = try Polygon.init(allocator, max_retries, thread_pool.threads.len, null);

    return Self{
        .allocator = allocator,
        .client = client,
        .progress = progress,
        .thread_pool = thread_pool,
        .outdir = outdir,
        .skip_trades = skip_trades,
    };
}

pub fn deinit(self: *Self) void {
    self.client.deinit();
}

pub fn download(self: *Self, date: []const u8) !void {
    const allocator = self.allocator;

    var grouped = try self.client.groupedDaily(date);
    defer grouped.deinit();

    var tickers: TickerSet = brk: {
        var res = TickerSet.init(allocator);

        if (grouped.value) |v| {
            for (v) |t| try res.put(t.T, {});
        }

        break :brk res;
    };
    defer tickers.deinit();

    log.info("{s} {d}", .{ date, tickers.unmanaged.size });

    if (tickers.unmanaged.size == 0) return;

    var new_test_tickers = try self.downloadTickers(date, tickers);
    defer new_test_tickers.deinit();

    var iter = new_test_tickers.keyIterator();
    while (iter.next()) |t| {
        log.info("new test ticker {s}", .{t.*});
        _ = tickers.remove(t.*);
    }

    if (!self.skip_trades) try self.downloadTrades(date, tickers);
}

fn columnIndex(columns: []const u8, column: []const u8) ?usize {
    var column_iter = std.mem.splitScalar(u8, columns, ',');
    var i: usize = 0;
    while (column_iter.next()) |c| : (i += 1) {
        if (std.mem.eql(u8, c, column)) return i;
    }

    return null;
}

fn createFile(self: *Self, dir: []const u8, date: []const u8) !std.fs.File {
    const allocator = self.allocator;
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}.csv.gz", .{ self.outdir, dir, date });
    defer allocator.free(path);

    const dirname = std.fs.path.dirname(path).?;
    try std.fs.cwd().makePath(dirname);

    return try std.fs.cwd().createFile(path, .{});
}

/// Caller owns returned TickerSet
fn downloadTickers(self: *Self, date: []const u8, tickers: TickerSet) !TickerSet {
    const allocator = self.allocator;

    var prog = self.progress.start("ticker details", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.createFile("tickers", date);
    defer out.close();

    var gzipped = try gzip.compress(allocator, out.writer(), .{});
    defer gzipped.deinit();

    const filed: FileWriter = gzipped.writer();

    var writer = csv_mod.csvWriter(TickerDetails, filed);
    try writer.writeHeader();

    var res = TickerSet.init(allocator);

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTickerWorker, .{
            self,
            &writer,
            date,
            t.*,
            &res,
            &prog,
        });
    }
    self.wait_group.wait();

    try gzipped.close();
    prog.end();
    if (prog.parent) |p| p.setCompletedItems(p.unprotected_completed_items - 1);

    return res;
}

fn downloadTickerWorker(
    self: *Self,
    writer: *csv_mod.CsvWriter(TickerDetails, FileWriter),
    date: []const u8,
    ticker: []const u8,
    test_tickers: *TickerSet,
    prog: *std.Progress.Node,
) void {
    defer {
        self.wait_group.finish();
        prog.completeOne();
    }

    var details = self.client.tickerDetails(ticker, date) catch |err| {
        log.err("error getting ticker details for {s}: {}", .{ ticker, err });
        unreachable;
    };
    defer details.deinit();

    if (details.value == null) return; // 404

    if (details.value.?.is_test) {
        test_tickers.put(ticker, {}) catch unreachable;
        return;
    }
    self.mutex.lock();
    defer self.mutex.unlock();

    writer.writeRecord(details.value.?) catch unreachable;
}

/// Caller owns returned TickerSet
fn downloadTrades(self: *Self, date: []const u8, tickers: TickerSet) !void {
    const allocator = self.allocator;

    var prog = self.progress.start("trades", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.createFile("trades", date);
    defer out.close();

    var gzipped = try gzip.compress(allocator, out.writer(), .{});
    defer gzipped.deinit();
    var writer: FileWriter = gzipped.writer();

    var sample = std.ArrayListUnmanaged(u8){};
    defer sample.deinit(allocator);

    try self.client.trades("A", date, sample.writer(allocator));
    var lines = std.mem.splitScalar(u8, sample.items, '\n');
    // conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
    const expected_columns = lines.first();

    try writer.print("ticker,{s}\n", .{expected_columns});

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

    try gzipped.close();
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

    const sink = TradesSink{
        .header = expected_header,
        .out = writer,
        .ticker = ticker,
        .mutex = &self.mutex,
    };

    self.client.trades(ticker, date, sink) catch |err| {
        log.err("error getting trades for {s}: {}", .{ ticker, err });
        unreachable;
    };
}
