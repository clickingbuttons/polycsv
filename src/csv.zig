const std = @import("std");

const Allocator = std.mem.Allocator;
pub const HeaderIndex = u31;

const ParsingError = error{
    MissingField,
};

pub const ReaderOptions = struct {
    header: bool = true,
    row_delimiter: u8 = '\n',
    field_delimiter: u8 = ',',
    quote_char: u8 = '"',
    array_delimiter: u8 = ',',
};

pub fn Reader(comptime ReaderType: type, comptime T: type) type {
    const struct_fields = std.meta.fields(T);

    return struct {
        allocator: Allocator,
        source: ReaderType,
        options: ReaderOptions,

        header_struct_field_indices: std.ArrayListUnmanaged(?HeaderIndex) = .{},
        field: std.ArrayListUnmanaged(u8) = .{},

        row_end: bool = false,

        const Self = @This();

        pub fn init(allocator: Allocator, source: ReaderType, options: ReaderOptions) Self {
            return Self{
                .allocator = allocator,
                .source = source,
                .options = options,
            };
        }

        pub fn deinit(self: *Self) void {
            self.header_struct_field_indices.deinit(self.allocator);
            self.field.deinit(self.allocator);
        }

        /// Reads header. If the passed struct has a value that is missing will throw.
        pub fn readHeader(self: *Self) !void {
            var found = [_]bool{false} ** struct_fields.len;

            // Given struct A,B,C
            // And header   D,C,B,A
            // Sets indices n,2,1,0
            while (try self.readField()) |s| {
                var index: ?HeaderIndex = null;
                inline for (struct_fields, 0..) |f, i| {
                    if (std.mem.eql(u8, s, f.name)) {
                        index = @intCast(i);
                        found[i] = true;
                        break;
                    }
                }
                try self.header_struct_field_indices.append(self.allocator, index);
            }

            inline for (struct_fields, 0..) |f, i| {
                if (f.default_value == null and !found[i]) {
                    std.log.err("CSV header missing struct field {s}\n", .{f.name});
                    return ParsingError.MissingField;
                }
            }
        }

        /// Caller owns returned slice.
        fn readField(self: *Self) !?[]const u8 {
            self.field.clearRetainingCapacity();

            if (self.row_end) {
                self.row_end = false;
                return null;
            }

            const State = enum {
                start,
                quote,
                quote_quote,
            };

            var state: State = .start;

            while (true) {
                const c = self.source.readByte() catch |err| switch (err) {
                    error.EndOfStream => {
                        if (self.field.items.len > 0) break;
                        return null;
                    },
                    else => |e| return e,
                };
                switch (state) {
                    .start => {
                        if (c == self.options.field_delimiter) break;
                        if (c == self.options.row_delimiter) {
                            self.row_end = true;
                            break;
                        }
                        if (c == self.options.quote_char) {
                            state = .quote;
                            self.field.clearRetainingCapacity();
                            continue;
                        }
                    },
                    .quote => {
                        if (c == self.options.quote_char) {
                            state = .quote_quote;
                            continue;
                        }
                    },
                    .quote_quote => {
                        // "asdf",
                        // "asdf"\n
                        if (c == self.options.field_delimiter) break;
                        if (c == self.options.row_delimiter) {
                            self.row_end = true;
                            break;
                        }
                        // "a""
                        if (c == self.options.quote_char) {
                            try self.field.append(self.allocator, '"');
                            state = .quote;
                            continue;
                        }
                        // "a"b
                        std.debug.print("unexpected char {c}\n", .{c});
                        return error.Parsing;
                    },
                }
                try self.field.append(self.allocator, c);
            }

            return self.field.items;
        }

        fn parse(self: Self, comptime U: type, value: []const u8) !U {
            const allocator = self.allocator;
            switch (@typeInfo(U)) {
                .Void => return {},
                .Bool => return !(std.mem.eql(u8, value, "false") or std.mem.eql(u8, value, "0")),
                .Int => return try std.fmt.parseInt(U, value, 10),
                .Float => return try std.fmt.parseFloat(U, value),
                .Pointer => |p| brk: {
                    if (p.size != .Slice) break :brk;

                    if (U == []const u8) return try allocator.dupe(u8, value);

                    var res = std.ArrayList(p.child).init(allocator);
                    errdefer res.deinit();
                    const v = if (value[0] == '[') value[1 .. value.len - 1] else value;
                    var split = std.mem.splitScalar(u8, v, self.options.array_delimiter);
                    while (split.next()) |n| {
                        const arr_item = try self.parse(p.child, n);
                        try res.append(arr_item);
                    }
                    return res.toOwnedSlice();
                },
                .Array => {
                    return null;
                },
                .Optional => |o| {
                    if (value.len == 0) return null;
                    return try self.parse(o.child, value);
                },
                // .Enum: Enum,
                // .Union: Union,
                // .Vector: Vector,
                else => {},
            }
            @compileError("csv parser cannot parse type " ++ @typeName(U));
        }

        pub fn next(self: *Self) !?T {
            if (self.header_struct_field_indices.items.len == 0) {
                inline for (0..struct_fields.len) |i| {
                    try self.header_struct_field_indices.append(self.allocator, i);
                }
            }

            var res: T = undefined;

            var field_i: usize = 0;
            while (try self.readField()) |s| : (field_i += 1) {
                const struct_index = self.header_struct_field_indices.items[field_i];
                if (struct_index) |si| {
                    inline for (struct_fields, 0..) |f, i| {
                        if (si == i) {
                            @field(res, f.name) = try self.parse(f.type, s);
                        }
                    }
                }
            }

            return if (field_i == 0) null else res;
        }
    };
}

pub fn reader(
    comptime T: type,
    allocator: Allocator,
    reader_: anytype,
    options: ReaderOptions,
) Reader(@TypeOf(reader_), T) {
    return Reader(@TypeOf(reader_), T).init(allocator, reader_, options);
}

fn expectHeader(comptime T: type, comptime expected: []const ?HeaderIndex, csv: []const u8) !void {
    const allocator = std.testing.allocator;
    var stream = std.io.fixedBufferStream(csv);

    var r = reader(T, allocator, stream.reader(), .{});
    defer r.deinit();
    try r.readHeader();

    try std.testing.expectEqualSlices(?HeaderIndex, expected, r.header_struct_field_indices.items);
}

test "header order" {
    try expectHeader(
        struct { A: u32, B: u32, C: u32 },
        &[_]?HeaderIndex{ 2, 0, 1 },
        "C,A,B",
    );

    try expectHeader(
        struct { A: u32, B: u32, C: u32 = 2 },
        &[_]?HeaderIndex{ 0, 1 },
        "A,B",
    );

    try std.testing.expectError(ParsingError.MissingField, expectHeader(
        struct { A: u32, B: u32, C: u32 },
        &[_]?HeaderIndex{ 0, 1 },
        "A,B",
    ));
}

fn expectStream(comptime T: type, comptime expected: []const T, csv: []const u8) !void {
    const allocator = std.testing.allocator;

    var stream = std.io.fixedBufferStream(csv);

    var r = reader(T, allocator, stream.reader(), .{});
    defer r.deinit();
    try r.readHeader();

    for (expected) |e| {
        const next = (try r.next()).?;
        try std.testing.expectEqualDeep(e, next);
        if (@hasDecl(@TypeOf(next), "deinit")) next.deinit(allocator);
    }

    try std.testing.expectEqual(@as(?T, null), try r.next());
}

test "1 row" {
    const ABC = struct { A: u32, B: u32, C: u32 };
    const csv =
        \\C,A,B
        \\1,2,3
    ;

    try expectStream(ABC, &[_]ABC{.{ .A = 2, .B = 3, .C = 1 }}, csv);
}

test "nullable row" {
    const ABC = struct { A: ?u32, B: u32, C: u32 };
    const csv =
        \\C,A,B
        \\1,,3
    ;

    try expectStream(ABC, &[_]ABC{.{ .A = null, .B = 3, .C = 1 }}, csv);
}

test "string" {
    const A = struct {
        A: []const u8,

        pub fn deinit(self: @This(), allocator: Allocator) void {
            allocator.free(self.A);
        }
    };
    const csv =
        \\A
        \\hello
    ;

    try expectStream(A, &[_]A{.{ .A = "hello" }}, csv);
}

test "field escaping" {
    const A = struct {
        A: []const u8,

        pub fn deinit(self: @This(), allocator: Allocator) void {
            allocator.free(self.A);
        }
    };
    const csv =
        \\A
        \\"hello, friend"
        \\"i said ""hello"""
    ;

    try expectStream(A, &[_]A{ .{ .A = "hello, friend" }, .{ .A = "i said \"hello\"" } }, csv);
}

test "slice" {
    const A = struct {
        A: []const u32,

        pub fn deinit(self: @This(), allocator: Allocator) void {
            allocator.free(self.A);
        }
    };
    const csv =
        \\A
        \\"[1,2,3]"
    ;

    try expectStream(A, &[_]A{.{ .A = &[_]u32{ 1, 2, 3 } }}, csv);
}
