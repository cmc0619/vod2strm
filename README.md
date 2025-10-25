# VOD STRM Generator Plugin - ThreadPool Version

This is the **ThreadPool** version of the VOD STRM Generator plugin for Dispatcharr. It uses multiple threads to write files concurrently, with configurable batching and throttling to prevent system overload.

## Key Features - ThreadPool Version

- **Multi-threaded File Operations**: Uses ThreadPoolExecutor to write files concurrently
- **Configurable Threading**: Adjust number of worker threads (1-10)
- **Batch Processing**: Process files in configurable batches to manage memory
- **Throttling**: Configurable delays between batches to prevent I/O overwhelm
- **Progress Logging**: Regular progress updates during processing
- **Resource Control**: Prevents the file-write storm that overwhelmed your system

## Configuration Options

### Standard Options
- **Output Directory**: Where to write .strm and .nfo files
- **Dispatcharr Base URL**: URL used in .strm files
- **Dry Run Mode**: Test without creating files
- **Verbose Logging**: Show per-item processing logs
- **Debug Logging**: Write detailed debug logs
- **Pre-populate Episodes**: Automatically fetch episode data

### ThreadPool-Specific Options
- **Max File Writer Threads** (default: 4): Number of concurrent file writing threads
- **Batch Size** (default: 100): Number of files to process in each batch
- **Throttle Delay** (default: 10ms): Delay between batches to prevent I/O overwhelm

## How It Works

1. **Preparation Phase**: Collects all file operations into a list (no actual writing)
2. **Batch Processing**: Splits file operations into manageable batches
3. **Threaded Execution**: Uses ThreadPoolExecutor to write files concurrently
4. **Progress Tracking**: Shows progress every batch completion
5. **Throttling**: Small delays between batches prevent system overload

## Performance Characteristics

**Pros:**
- Much faster than synchronous writing
- Controllable resource usage via thread count and batching
- Immediate feedback and progress updates
- No external dependencies

**Cons:**
- Still runs in the plugin's `run()` method (blocking UI until complete)
- Limited by Python's GIL for CPU-bound operations
- May timeout on very large libraries

## Recommended Settings

**For Small Libraries (<1000 files):**
- Max Threads: 4
- Batch Size: 50
- Throttle Delay: 5ms

**For Medium Libraries (1000-5000 files):**
- Max Threads: 4-6
- Batch Size: 100
- Throttle Delay: 10ms

**For Large Libraries (>5000 files):**
- Max Threads: 6-8
- Batch Size: 100-200
- Throttle Delay: 20ms

## Installation

Same as the standard version - copy to `/data/plugins/vod2strm_threadpool/`

## Use This Version If:

- You want faster processing than the original synchronous version
- You need immediate feedback and progress logging
- You want to stay within the standard plugin architecture
- You prefer simplicity over the complexity of Celery tasks
