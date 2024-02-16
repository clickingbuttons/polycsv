const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const argparser = b.dependency("argparser", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "polycsv",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("argparser", argparser.module("simargs"));
    b.installArtifact(exe);

    const lib = b.addStaticLibrary(.{
        .name = "regex_slim",
        .optimize = .Debug,
        .target = target,
    });
    lib.addIncludePath(.{ .path = "lib" });
    lib.addCSourceFiles(.{
        .files = &.{"lib/regex_slim.c"},
        .flags = &.{"-std=c99"},
    });
    lib.linkLibC();
    exe.linkLibrary(lib);
    exe.addIncludePath(.{ .path = "lib" });
    exe.linkLibC();

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    unit_tests.linkLibrary(lib);
    unit_tests.addIncludePath(.{ .path = "lib" });
    unit_tests.linkLibC();

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
