package org.apache.spark.memory;

public class SigVE {
	public static volatile boolean handling_signal = false;
	public static volatile long last_signal = 0;
	public static volatile long start_signal = 0;
	public static volatile long epoch = 1;
	public static volatile long allocations = 0;
	/*
	 * 72 seconds
	 */
	public static long N = 1; // N = 5
	public static long budget = 0;
	public static final Object lock = new Object();
	private static MemoryManager mm = null;

	public static boolean enabled() {
		java.util.List<String> args = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments();
		for (String s : args) {
			if (s.equals("-XX:+UseSIGVE")) {
				System.out.print("[sigve] SigVE enabled.\n");
				SigVE.debug();
				return true;
			}
		}
		System.out.print("[sigve] SigVE disabled.\n");
		SigVE.debug();
		return false;
	}

	public static void debug() {
		/*
		try {
			for (String line : java.nio.file.Files.readAllLines(new java.io.File("/proc/self/status").toPath(), java.nio.charset.Charset.forName("utf-8"))) {
				if (line.startsWith("VmRSS")) {
					System.out.print("[sigve] /proc/self/status " + line + "\n");
				}
			}
			for (String line : java.nio.file.Files.readAllLines(new java.io.File("/proc/self/stat").toPath(), java.nio.charset.Charset.forName("utf-8"))) {
				System.out.print("[sigve] /proc/self/stat: " + line + "\n");
			}
		} catch (java.io.IOException ex) {
			System.out.print("[sigve] debug error");
			ex.printStackTrace();
		}
		*/
	}

	public static void set_mm(MemoryManager mm) {
		synchronized (lock) {
			if (SigVE.mm != null) {
				throw new RuntimeException("multiple memory managers");
				//System.out.print("[sigve] multiple memory managers\n");
				//Thread.dumpStack();
			}
			SigVE.mm = mm;
		}
	}

	public static void okotemasu() {
		if (!SigVE.enabled()) {
			return;
		}
		System.out.print("[sigve] SigVE.okotemasu\n");
		//long t0 = System.currentTimeMillis();
		synchronized (lock) {
			SigVE.last_signal = System.currentTimeMillis();
			if (SigVE.handling_signal) {
				return;
			}
			SigVE.handling_signal = true;
			SigVE.start_signal = SigVE.last_signal;
		}
		if (SigVE.mm != null) {
			long t1 = System.currentTimeMillis();
			SigVE.mm.sigveShrinkStoragePool(MemoryMode.ON_HEAP);
			long t2 = System.currentTimeMillis();
			System.out.print("[sigve] should_behave " + (t2 - t1) + "\n");
		} else {
			System.out.print("[sigve] mm null\n");
		}
		long t3 = System.currentTimeMillis();
		System.gc();
		long t4 = System.currentTimeMillis();
		System.out.print("[sigve] gc took " + (t4 - t3) + "\n");
		synchronized (lock) {
			SigVE.epoch = System.currentTimeMillis() - SigVE.start_signal;
			SigVE.handling_signal = false;
		}
	}

	public static boolean can_grow(Long memoryUsed, Long numBytesToAcquire) {
		long t0 = System.currentTimeMillis();
		System.out.print("[sigve] can_grow acquiring lock\n");
		synchronized (lock) {
			long t1 = System.currentTimeMillis();
			System.out.print("[sigve] can_grow got lock " + (t1 - t0) + "\n");
			if (numBytesToAcquire <= SigVE.budget) {
				System.out.print("[sigve] can_grow allowing by budget " + SigVE.budget + ", to acquire " + numBytesToAcquire + "\n");
				SigVE.budget -= numBytesToAcquire;
				return true;
			}
			double x = (double) (t1 - SigVE.last_signal) / SigVE.epoch;
			double f = SigVE.N / x;
			System.out.print("[sigve] SigVE.epoch " + SigVE.epoch + " ms, t1 - last rage " + (t1 - SigVE.last_signal) + " ms, x " + x + ", f " + f + ", allocations " + SigVE.allocations + ", toAcquire " + numBytesToAcquire + ".\n");
			if (SigVE.allocations >= Math.floor(f)) {
				SigVE.allocations = 0;
				SigVE.budget = (memoryUsed / 4) - numBytesToAcquire;
				long t2 = System.currentTimeMillis();
				System.out.print("[sigve] can_grow returning " + (t2 - t0) + "\n");
				return true;
			}
			SigVE.allocations++;
		}
		long t2 = System.currentTimeMillis();
		System.out.print("[sigve] can_grow returning " + (t2 - t0) + "\n");
		return false;
	}
}
