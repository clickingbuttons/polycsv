const std = @import("std");
const Polygon = @import("./polygon.zig");

const Allocator = std.mem.Allocator;
const http = std.http;
const key_var = "POLYGON_KEY";
const Container = Polygon.Container;

fn panic(comptime format: []const u8, args: anytype) void {
    std.log.err(format, args);
    std.process.exit(1);
    noreturn;
}

fn readAll(allocator: Allocator, fname: []const u8) ![]const u8 {
    const file = try std.fs.cwd().openFile(fname, .{});
    defer file.close();

    return try file.reader().readAllAlloc(allocator, 1 << 32);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const test_tickers = try readAll(allocator, "test_tickers.txt");
    defer allocator.free(test_tickers);

    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const key = env.get(key_var) orelse return panic("set env var {s}", .{ key_var });

    var client = try Polygon.init(allocator, key);
    defer client.deinit();

    const date = "2023-01-09";

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{ .allocator = allocator });
    defer thread_pool.deinit();

    var ticker_details = try tickerDetails(&client, &thread_pool, test_tickers, date);
    defer ticker_details.deinit(allocator);
}

/// Caller owns returned container.
fn tickerDetails(
    client: *Polygon,
    thread_pool: *std.Thread.Pool,
    test_tickers: []const u8,
    date: []const u8,
) !Container(Polygon.TickerDetails) {
    const allocator = client.client.allocator;

    // 1. Get active tickers from grouped_daily
    var progress = std.Progress{};
    progress.log("{s}\n", .{ date });
    var root_node = progress.start("Get active tickers", 1);

    var active = try client.grouped_daily(date);
    defer active.deinit(allocator);

    progress.refresh();
    root_node.end();

    // 2. Remove ones that are in test_tickers list. Use `n` field to represent if is test.
    var n_active: usize = active.items.len;
    for (active.items) |*a| {
        a.n = 0;
        var split = std.mem.splitScalar(u8, test_tickers, '\n');
        while (split.next()) |t| {
            if (std.mem.eql(u8, a.T, t)) {
                a.n = 1;
                n_active -= 1;
                break;
            }
        }
    }

    // 3. Get ticker details for rest. Only add ones that are not tests.
    var res = try std.ArrayListUnmanaged(Polygon.TickerDetails).initCapacity(allocator, n_active);
    var mutex = std.Thread.Mutex{};

    var wait_group: std.Thread.WaitGroup = .{};

    root_node = progress.start("Ticker details", n_active);
    for (active.items[0..10]) |*a| {
        if (a.n == 0) {
            wait_group.start();
            try thread_pool.spawn(workerAst, .{
                client,
                root_node,
                date,
                a,
                &wait_group,
                &res,
                &mutex,
            });
        }
    }
    wait_group.wait();
    root_node.end();

    const items = try res.toOwnedSlice(allocator);
    return Container(Polygon.TickerDetails) { .items = items };
}

fn workerAst(
    client: *Polygon,
    progress: *std.Progress.Node,
    date: []const u8,
    a: *Polygon.GroupedDaily,
    wait_group: *std.Thread.WaitGroup,
    items: *std.ArrayListUnmanaged(Polygon.TickerDetails),
    mutex: *std.Thread.Mutex,
) void {
    defer wait_group.finish();
    const allocator = client.client.allocator;

    var details = client.tickerDetails(a.T, date) catch |err| {
        // if (err == error.RateLimit);
        std.debug.print("err {}\n", .{ err });
        return;
    };
    mutex.lock();
    defer mutex.unlock();
    if (details) |d| {
        if (d.is_test != null and d.is_test == true) {
            a.n = 1;
            return;
        }
        items.append(allocator, d) catch unreachable;
    } else {
        const duped = allocator.dupe(u8, a.T) catch unreachable;
        items.append(allocator, Polygon.TickerDetails{ .ticker = duped, }) catch unreachable;
    }
    progress.completeOne();
}

test {
    _ = @import("./csv.zig");
}
