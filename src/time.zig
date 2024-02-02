const std = @import("std");
const epoch = std.time.epoch;

pub const Weekday = enum(u3) {
    Sunday = 0,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
};

pub const Date = struct {
    year: epoch.Year,
    month: epoch.Month,
    // 1 - 31
    day: u8,

    const Self = @This();

    pub fn now() Self {
        const epoch_seconds = @divTrunc(std.time.milliTimestamp(), 1000);
        const secs = epoch.EpochSeconds{ .secs = @intCast(epoch_seconds) };
        const day = secs.getEpochDay();
        const year_day = day.calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        return Self{
            .year = year_day.year,
            .month = month_day.month,
            .day = @intCast(month_day.day_index + 1),
        };
    }

    /// Doesn't follow any spec. Just expects YYYY-mm-dd
    pub fn parse(buf: []const u8) !Self {
        if (buf.len < 10) return error.TooShort;
        const year = try std.fmt.parseInt(epoch.Year, buf[0..4], 10);
        const month = try std.fmt.parseInt(u4, buf[5..7], 10);
        const day = try std.fmt.parseInt(u8, buf[8..10], 10);
        return Self{
            .year = year,
            .month = @enumFromInt(month),
            .day = day,
        };
    }

    pub fn bufPrint(self: Self, buf: []u8) !void {
        _ = try std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{
            self.year,
            self.month.numeric(),
            self.day,
        });
    }

    pub fn increment(self: *Self) void {
        self.day += 1;

        const leap_kind: epoch.YearLeapKind = if (epoch.isLeapYear(self.year)) .leap else .not_leap;
        const days_in_month = epoch.getDaysInMonth(leap_kind, self.month);

        if (self.day >= days_in_month) {
            self.day = 1;
            if (self.month == .dec) {
                self.year += 1;
                self.month = .jan;
            } else {
                self.month = @enumFromInt(self.month.numeric() + 1);
            }
        }
    }

    pub fn weekday(self: Self) Weekday {
        // For year > 1752
        var y = self.year;
        const m = self.month.numeric();
        const d = self.day;
        const t = [_]u8{ 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };
        if (m < 3) y -= 1;
        const sum = (y + y / 4 - y / 100 + y / 400 + t[m - 1] + d);
        const lowered: u3 = @intCast(sum % 7);
        return @enumFromInt(lowered);
    }

    pub fn isWeekend(self: Self) bool {
        const w = self.weekday();
        return w == .Saturday or w == .Sunday;
    }
};

fn testWeekday(year: epoch.Year, month: epoch.Month, day: u8, expected: Weekday) !void {
    const date = Date{ .year = year, .month = month, .day = day };
    try std.testing.expectEqual(expected, date.weekday());
}

test "weekday" {
    try testWeekday(2003, .sep, 10, Weekday.Wednesday);
    try testWeekday(2003, .sep, 13, Weekday.Saturday);
    try testWeekday(2024, .feb, 1, Weekday.Thursday);
    try testWeekday(2024, .feb, 3, Weekday.Saturday);
    try testWeekday(2024, .feb, 4, Weekday.Sunday);
}
