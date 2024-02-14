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

        fn writeValue(self: *Self, value: anytype) !void {
            const ti = @typeInfo(@TypeOf(value));

            switch (ti) {
                .Bool => try self.writer.writeAll(if (value) "true" else "false"),
                .Int, .Float => try self.writer.print("{d}", .{value}),
                .Optional => {
                    if (value != null) try self.writeValue(value.?);
                },
                .Pointer => |p| switch (p.size) {
                    .Slice => {
                        if (p.child != u8) @compileError("unsupported slice type " ++ @typeName(p.child));
                        if (value.len > 0) {
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
                        }
                    },
                    else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
                },
                .Struct => |s| {
                    inline for (s.fields) |f| {
                        try self.writeValue(@field(value, f.name));
                    }
                },
                else => |t| @compileError("cannot serialize" ++ @typeName(t)),
            }

            switch (ti) {
                .Optional, .Struct => {},
                else => {
                    self.field_i += 1;
                    if (self.field_i != n_fields) try self.writer.writeByte(self.field_delim);
                },
            }
        }

        pub fn writeRecord(self: *Self, record: T) !void {
            self.field_i = 0;
            try self.writer.writeByte(self.record_delim);
            try self.writeValue(record);
        }
    };
}

pub fn csvWriter(comptime T: type, child_writer: anytype) CsvWriter(T, @TypeOf(child_writer)) {
    return CsvWriter(T, @TypeOf(child_writer)){ .writer = child_writer };
}
