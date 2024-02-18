const std = @import("std");
const Polygon = @import("./polygon.zig");
const csv_mod = @import("./CsvWriter.zig");
const TickerRegexes = @import("./Regex.zig").TickerRegexes;
const time = @import("./time.zig");

pub const TickerSet = std.StringHashMap(void);
const log = std.log;
const Allocator = std.mem.Allocator;
const gzip = std.compress.gzip;
const FileWriter = gzip.Compressor(std.fs.File.Writer).Writer;
const TickerDetails = Polygon.TickerDetails;

allocator: Allocator,
client: Polygon,
progress: *std.Progress.Node,
thread_pool: *std.Thread.Pool,
wait_group: std.Thread.WaitGroup = .{},
mutex: std.Thread.Mutex = .{},
outdir: []const u8,
skip_trades: bool,
ticker_regexes: TickerRegexes,

const Self = @This();

pub fn init(
    allocator: Allocator,
    thread_pool: *std.Thread.Pool,
    progress: *std.Progress.Node,
    outdir: []const u8,
    max_retries: usize,
    skip_trades: bool,
    test_tickers_path: []const u8,
) !Self {
    const client = try Polygon.init(allocator, max_retries, thread_pool.threads.len, null);

    return Self{
        .allocator = allocator,
        .client = client,
        .progress = progress,
        .thread_pool = thread_pool,
        .outdir = outdir,
        .skip_trades = skip_trades,
        .ticker_regexes = try TickerRegexes.init(allocator, test_tickers_path),
    };
}

pub fn deinit(self: *Self) void {
    self.client.deinit();
    self.ticker_regexes.deinit();
}

pub fn download(self: *Self, date: []const u8) !void {
    const allocator = self.allocator;

    log.info("{s}", .{ date });

    var grouped = try self.client.groupedDaily(date);
    defer grouped.deinit();

    var tickers: TickerSet = brk: {
        var res = TickerSet.init(allocator);
        const parsed = try time.Date.parse(date);

        if (grouped.value) |v| {
            for (v) |t| {
                if (self.ticker_regexes.matches(t.T, parsed)) {
                    std.log.info("skipping test ticker {s}", .{ t.T });
                } else {
                    try res.put(t.T, {});
                }
            }
        }

        break :brk res;
    };
    defer tickers.deinit();

    log.info("{s} {d} tickers", .{ date, tickers.unmanaged.size });

    if (tickers.unmanaged.size == 0) return;

    try self.downloadTickers(date, tickers);

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
fn downloadTickers(self: *Self, date: []const u8, tickers: TickerSet) !void {
    var prog = self.progress.start("ticker details", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.createFile("tickers", date);
    defer out.close();

    var gzipped = try gzip.compressor(out.writer(), .{});

    const filed: FileWriter = gzipped.writer();

    var writer = csv_mod.csvWriter(TickerDetails, filed);
    try writer.writeHeader();

    self.wait_group.reset();
    var keys = tickers.keyIterator();
    while (keys.next()) |t| {
        self.wait_group.start();
        try self.thread_pool.spawn(downloadTickerWorker, .{
            self,
            &writer,
            date,
            t.*,
            &prog,
        });
    }
    self.wait_group.wait();

    try gzipped.finish();
    prog.end();
    if (prog.parent) |p| p.setCompletedItems(p.unprotected_completed_items - 1);
}

fn downloadTickerWorker(
    self: *Self,
    writer: *csv_mod.CsvWriter(TickerDetails, FileWriter),
    date: []const u8,
    ticker: []const u8,
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

    if (details.value) |v| {
        self.mutex.lock();
        defer self.mutex.unlock();

        writer.writeRecord(v) catch unreachable;
    }
}

/// Caller owns returned TickerSet
fn downloadTrades(self: *Self, date: []const u8, tickers: TickerSet) !void {
    const allocator = self.allocator;

    var prog = self.progress.start("trades", tickers.unmanaged.size);
    prog.setUnit(" tickers");
    prog.activate();

    var out = try self.createFile("trades", date);
    defer out.close();

    var gzipped = try gzip.compressor(out.writer(), .{});
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

    try gzipped.finish();
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
