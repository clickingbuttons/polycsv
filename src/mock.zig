const std = @import("std");
const websocket = @import("websocket");
const Conn = websocket.Conn;
const Message = websocket.Message;
const Handshake = websocket.Handshake;
const RndGen = std.rand.DefaultPrng;

const address = "127.0.0.1";
const port = 8080;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    std.debug.print("listening on {s}:{d} \n",.{ address, port});
    try websocket.listen(Handler, allocator, {}, .{ .address = address, .port = port });
}

const Handler = struct {
    conn: *Conn,
    n_recv: usize = 0,

    pub fn init(_: Handshake, conn: *Conn, _: void) !Handler {
        return Handler{ .conn = conn };
    }

    pub fn afterInit(self: *Handler) !void {
        try self.conn.write("welcome");
    }

    pub fn handle(self: *Handler, message: Message) !void {
        _ = message;

        switch (self.n_recv) {
            0 => try self.conn.write("authed"),
            1 => {
                const trade =
                    \\{"ev":"T","sym":"HIVE","i":"1223","x":11,"p":4.08,"s":200,"t":1708453945556,"q":4216525,"z":3}
                    ;
                try self.conn.write("subscribed");
                var rnd = RndGen.init(0);

                const max_trades = 100;
                var send_buf: [(trade.len + 1) * max_trades + 2]u8 = undefined;
                var stream = std.io.fixedBufferStream(&send_buf);
                var writer = stream.writer();

                var n_sent: usize = 1;
                while (true) : (n_sent += 1) {
                    const n_trades = switch (n_sent % 10_000) {
                        0 => max_trades,
                        else => rnd.random().intRangeLessThan(usize, 1, 10),
                    };

                    stream.reset();
                    try writer.writeAll("[");
                    for (0..n_trades) |i| {
                        try writer.writeAll(trade);
                        if (i != n_trades - 1) try writer.writeAll(",");
                    }
                    try writer.writeAll("]");
                    try self.conn.write(send_buf[0..stream.pos]);
                    std.time.sleep(std.time.ns_per_s / 10_000);
                }
            },
            else => try self.conn.write("go away"),
        }
        self.n_recv += 1;
    }

    // called whenever the connection is closed, can do some cleanup in here
    pub fn close(_: *Handler) void {
        std.debug.print("closed\n", .{});
    }
};
