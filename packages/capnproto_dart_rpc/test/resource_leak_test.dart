import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

void main() {
  test(
    'UDS RPC load does not retain RSS, heap, capabilities, or connections',
    () async {
      final currentDirectory = Directory.current;
      final packageDirectory =
          File('test/resource/uds_resource_leak_worker.dart').existsSync()
              ? currentDirectory
              : Directory(
                '${currentDirectory.path}/packages/capnproto_dart_rpc',
              );
      final process = await Process.start(Platform.resolvedExecutable, const [
        '--enable-vm-service=0',
        '--disable-service-auth-codes',
        'run',
        'test/resource/uds_resource_leak_worker.dart',
      ], workingDirectory: packageDirectory.path);
      final stdoutFuture = process.stdout.transform(utf8.decoder).join();
      final stderrFuture = process.stderr.transform(utf8.decoder).join();
      final exitCode = await process.exitCode.timeout(
        const Duration(minutes: 3),
        onTimeout: () {
          process.kill();
          throw TimeoutException('UDS resource leak worker timed out');
        },
      );
      final stdoutText = await stdoutFuture;
      final stderrText = await stderrFuture;

      expect(
        exitCode,
        0,
        reason: 'worker stdout:\n$stdoutText\nworker stderr:\n$stderrText',
      );

      final jsonLine =
          stdoutText
              .split('\n')
              .where((line) => line.trimLeft().startsWith('{'))
              .last;
      final result = jsonDecode(jsonLine) as Map<String, dynamic>;
      expect(result['transport'], 'uds');
      expect(result['operations'], 14000);
      expect(result['rpcCalls'], greaterThan(14000));
      expect(result['capabilityTransfers'], 1400);
      expect(result['pipelinedCalls'], 1400);
      expect(result['streamingCalls'], 2800);
      expect(result['connectionCycles'], 5);
      expect(result['weakReferencesCollected'], isTrue);
    },
    skip: Platform.isWindows ? 'Unix domain sockets are unavailable' : false,
    timeout: const Timeout(Duration(minutes: 4)),
  );
}
