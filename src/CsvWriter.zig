const std = @import("std");

fn nFields(comptime T: type) usize {
    switch (@typeInfo(T)) {
        .Struct => |s| {
            var res: usize = 0;
            inline for (s.fields) |f| res += nFields(f.type);
            return res;
        },
        else => return 1,
    }
}

/// Flattens structs by omitting parent struct's name. Does not check child struct fields for uniqueness.
pub fn CsvWriter(comptime T: type, comptime WriterType: type) type {
    const n_fields = nFields(T);

    return struct {
        writer: WriterType,
        record_delim: u8 = '\n',
        field_delim: u8 = ',',
        quote: u8 = '"',

        field_i: usize = 0,

        const Self = @This();

        fn writeHeaderType(self: *Self, comptime U: type) !void {
            const fields = std.meta.fields(U);
            inline for (fields) |f| {
                switch (@typeInfo(f.type)) {
                    .Struct => try self.writeHeaderType(f.type),
                    else => {
                        try self.writer.writeAll(f.name);
                        self.field_i += 1;
                        if (self.field_i != n_fields) try self.writer.writeByte(self.field_delim);
                    },
                }
            }
        }

        pub fn writeHeader(self: *Self) !void {
            self.field_i = 0;
            try self.writeHeaderType(T);
        }

        fn writeValue(self: *Self, value: anytype, is_child: bool) !void {
            switch (@typeInfo(@TypeOf(value))) {
                .Bool => try self.writer.writeAll(if (value) "true" else "false"),
                .Int, .Float => try self.writer.print("{d}", .{value}),
                .Optional => {
                    if (value != null) try self.writeValue(value.?, true);
                },
                .Pointer => |p| switch (p.size) {
                    .Slice => brk: {
                        if (value.len == 0) break :brk;
                        // Treat as string
                        if (p.child == u8 and p.is_const) {
                            const needs_escape =
                                std.mem.indexOfScalar(u8, value, self.field_delim) != null or
                                std.mem.indexOfScalar(u8, value, self.quote) != null;

                            if (needs_escape) {
                                try self.writer.writeByte(self.quote);
                                for (value) |c| {
                                    if (c == self.quote) try self.writer.writeByte(self.quote);
                                    try self.writer.writeByte(c);
                                }
                                try self.writer.writeByte(self.quote);
                            } else {
                                try self.writer.writeAll(value);
                            }
                        } else {
                            // Treat as slice
                            const needs_escape = value.len > 1;
                            if (needs_escape) try self.writer.writeByte(self.quote);
                            for (value, 0..) |v, i| {
                                try self.writeValue(v, true);
                                if (i != value.len - 1) try self.writer.writeByte(self.field_delim);
                            }
                            if (needs_escape) try self.writer.writeByte(self.quote);
                        }
                    },
                    else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
                },
                .Struct => |s| {
                    inline for (s.fields) |f| try self.writeValue(@field(value, f.name), true);
                },
                else => |t| @compileError("cannot serialize " ++ @tagName(t)),
            }

            if (!is_child) {
                self.field_i += 1;
                if (self.field_i != n_fields) try self.writer.writeByte(self.field_delim);
            }
        }

        pub fn writeRecord(self: *Self, record: T) !void {
            self.field_i = 0;
            try self.writer.writeByte(self.record_delim);
            inline for (std.meta.fields(T)) |f| try self.writeValue(@field(record, f.name), false);
        }
    };
}

pub fn csvWriter(comptime T: type, child_writer: anytype) CsvWriter(T, @TypeOf(child_writer)) {
    return CsvWriter(T, @TypeOf(child_writer)){ .writer = child_writer };
}

test "nested struct with optionals" {
    const T = struct {
        ticker: []const u8,
        address: struct {
            address1: []const u8,
            address2: []const u8,
        },
        shares_outstanding: ?usize,
        description: []const u8,
    };
    const t = T{ .ticker = "AAPL", .address = .{
        .address1 = "ONE APPLE PARK WAY",
        .address2 = "TWO APPLE PARK WAY",
    }, .shares_outstanding = null, .description = "crapple" };

    const allocator = std.testing.allocator;
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    var writer = csvWriter(T, list.writer());
    try writer.writeHeader();
    try writer.writeRecord(t);

    const expected =
        \\ticker,address1,address2,shares_outstanding,description
        \\AAPL,ONE APPLE PARK WAY,TWO APPLE PARK WAY,,crapple
    ;
    try std.testing.expectEqualStrings(expected, list.items);
}
