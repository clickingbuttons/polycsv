const std = @import("std");

pub fn entry(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib: *std.Build.Step.Compile,
    all_unit_tests: *std.Build.Step,
    comptime name: []const u8,
) void {
    const argparser = b.dependency("argparser", .{
        .target = target,
        .optimize = optimize,
    });
    // const websocket = b.dependency("websocket", .{
    //     .target = target,
    //     .optimize = optimize,
    // });
    // for (websocket.builder.modules.keys()) |k| std.debug.print("{s}\n", .{ k });

    const entry_path = "src/" ++ name ++ ".zig";
    const exe = b.addExecutable(.{
        .name = name,
        .root_source_file = .{ .path = entry_path },
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("argparser", argparser.module("simargs"));
    // exe.root_module.addImport("websocket", websocket.module("ws"));
    exe.linkLibrary(lib);
    exe.addIncludePath(.{ .path = "lib" });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run-" ++ name, "Run " ++ name);
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_source_file = .{ .path = entry_path },
        .target = target,
        .optimize = optimize,
    });
    unit_tests.linkLibrary(lib);
    unit_tests.addIncludePath(.{ .path = "lib" });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test-" ++ name, "Run " ++ name ++ " unit tests");
    test_step.dependOn(&run_unit_tests.step);

    all_unit_tests.dependOn(&run_unit_tests.step);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

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

    const all_test_step = b.step("test", "Run ALL unit tests");

    entry(b, target, optimize, lib, all_test_step, "backfill");
    entry(b, target, optimize, lib, all_test_step, "clean");
    entry(b, target, optimize, lib, all_test_step, "stream");
}
