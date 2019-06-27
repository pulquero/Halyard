package com.msd.gin.halyard.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public final class MultiResultScanner implements ResultScanner {

	private final CompletionService<AsyncTableScanner> completionService;
	private final List<? extends AsyncTableScanner> scanners;
	private int scannersRemaining;
	private AsyncTableScanner currentScanner;

	public MultiResultScanner(CompletionService<AsyncTableScanner> completionService, List<? extends AsyncTableScanner> scanners) {
		this.completionService = completionService;
		this.scanners = scanners;
		this.scannersRemaining = scanners.size();
	}

	@Override
	public Result next() throws IOException {
		while(true) {
			if (currentScanner == null) {
				if (scannersRemaining > 0) {
					try {
						Future<AsyncTableScanner> f = completionService.take();
						scannersRemaining--;
						currentScanner = f.get();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return null;
					} catch (ExecutionException e) {
						if (e.getCause() instanceof IOException) {
							throw (IOException) e.getCause();
						} else if (e.getCause() instanceof RuntimeException) {
							throw (RuntimeException) e.getCause();
						} else {
							throw new AssertionError(e);
						}
					}
				} else {
					return null;
				}
			}
			Result res = currentScanner.next();
			if (res == null) {
				currentScanner.close();
				currentScanner = null;
			} else {
				return res;
			}
		}
	}

	@Override
	public void close() {
		for (AsyncTableScanner scanner : scanners) {
			scanner.close();
		}
	}

	@Override
	public Result[] next(int nbRows) throws IOException {
		// Collect values to be returned here
		ArrayList<Result> resultSets = new ArrayList<>(nbRows);
		for (int i = 0; i < nbRows; i++) {
			Result next = next();
			if (next != null) {
				resultSets.add(next);
			} else {
				break;
			}
		}
		return resultSets.toArray(new Result[resultSets.size()]);
	}

	@Override
	public Iterator<Result> iterator() {
		return new Iterator<Result>() {
			// The next RowResult, possibly pre-read
			Result next = null;

			// return true if there is another item pending, false if there isn't.
			// this method is where the actual advancing takes place, but you need
			// to call next() to consume it. hasNext() will only advance if there
			// isn't a pending next().
			@Override
			public boolean hasNext() {
				if (next == null) {
					try {
						next = MultiResultScanner.this.next();
						return next != null;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				return true;
			}

			// get the pending next item and advance the iterator. returns null if
			// there is no next item.
			@Override
			public Result next() {
				// since hasNext() does the real advancing, we call this to determine
				// if there is a next before proceeding.
				if (!hasNext()) {
					return null;
				}

				// if we get to here, then hasNext() has given us an item to return.
				// we want to return the item and then null out the next pointer, so
				// we use a temporary variable.
				Result temp = next;
				next = null;
				return temp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	static final class AsyncTableScanner implements Callable<AsyncTableScanner>, Closeable {
		private final TableProvider tableProvider;
		private final Scan scan;
		private volatile boolean isClosed;
		private volatile Table table;
		private volatile ResultScanner scanner;

		AsyncTableScanner(TableProvider tableProvider, Scan scan) {
			this.tableProvider = tableProvider;
			this.scan = scan;
		}

		public AsyncTableScanner call() throws IOException {
			if (isClosed) {
				return this;
			}
			table = tableProvider.getTable();
			if (isClosed) {
				return this;
			}
			synchronized (this) {
				if (table != null) {
					scanner = table.getScanner(scan);
				}
			}
			return this;
		}

		public Result next() throws IOException {
			return (scanner != null) ? scanner.next() : null;
		}

		public void close() {
			isClosed = true;

			synchronized (this) {
				if (scanner != null) {
					scanner.close();
					scanner = null;
				}

				if (table != null) {
					try {
						table.close();
					} catch (IOException e) {
						// close silently
					}
					table = null;
				}
			}
		}
	}
}
