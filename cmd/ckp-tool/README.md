# ckp-tool

A command-line tool for reading and modifying checkpoint meta files in MatrixOne.

## Overview

`ckp-tool` is a utility tool designed to inspect and manipulate checkpoint meta files. It provides functionality to:
- Read and display checkpoint meta file contents in a human-readable format
- Delete specific rows from checkpoint meta files by index or by matching end_ts value

## Building

To build the tool:

```bash
make ckp-tool
```

The binary will be generated as `ckp-tool` in the project root directory.

## Usage

### Command Structure

The tool uses a subcommand-based interface:

```
ckp-tool <command> [flags]
```

### Commands

#### `read` - Read checkpoint meta file

Reads and displays the contents of a checkpoint meta file.

**Syntax:**
```bash
ckp-tool read -file <path> [-s3=true|false]
```

**Flags:**
- `-file` (required): Path to the checkpoint meta file
- `-s3` (optional): Use S3 mode (DISK backend) to skip checksum validation (default: `true`)

**Example:**
```bash
# Read a checkpoint file
ckp-tool read -file ./meta_123_456.ckp

# Read with LocalFS mode (with checksum validation)
ckp-tool read -file ./meta_123_456.ckp -s3=false
```

**Output:**
The tool displays detailed information about each row in the checkpoint file, including:
- `start_ts`: Start timestamp
- `end_ts`: End timestamp
- `meta_location`: Meta location information
- `entry_type`: Entry type (true=incremental, false=global)
- `version`: Version number
- `all_locations`: All locations information
- `checkpoint_lsn`: Checkpoint LSN
- `truncate_lsn`: Truncate LSN
- `type`: Entry type (ET_Global, ET_Incremental, ET_Compacted, ET_Backup)

#### `delete` - Delete rows from checkpoint meta file

Deletes one or more rows from a checkpoint meta file. You can specify the row to delete either by index or by matching the `end_ts` value.

**Syntax:**
```bash
ckp-tool delete -file <path> [-index <row> | -end-ts <ts>] [-output <path>] [-s3=true|false]
```

**Flags:**
- `-file` (required): Path to the checkpoint meta file
- `-index` (optional): Row index to delete (0-based). Mutually exclusive with `-end-ts`
- `-end-ts` (optional): Delete row matching this end_ts value (e.g., `'1766148146913695795-1'`). Mutually exclusive with `-index`
- `-output` (optional): Output file path. If not specified, the original file will be overwritten
- `-s3` (optional): Use S3 mode (DISK backend) to skip checksum validation (default: `true`)

**Note:** Either `-index` or `-end-ts` must be specified.

**Examples:**

```bash
# Delete row by index (delete the first row, index 0)
ckp-tool delete -file ./meta_123_456.ckp -index 0

# Delete row by end_ts value
ckp-tool delete -file ./meta_123_456.ckp -end-ts '1766148146913695795-1'

# Delete row and save to a new file
ckp-tool delete -file ./meta_123_456.ckp -index 1 -output ./meta_new.ckp

# Delete with LocalFS mode
ckp-tool delete -file ./meta_123_456.ckp -index 0 -s3=false
```

**Verification:**
After deletion, the tool automatically verifies the result by reading the modified file and displaying its contents.

### `help` - Show help message

Displays the help message with usage information.

```bash
ckp-tool help
# or
ckp-tool -h
# or
ckp-tool --help
```

## File Modes

### S3 Mode (Default)

- Uses S3FS with DISK endpoint
- Skips checksum validation
- Recommended for corrupted or modified files
- Default mode: `-s3=true`

### LocalFS Mode

- Uses local filesystem
- Performs checksum validation
- Use when you want to verify file integrity
- Enable with: `-s3=false`

## Checkpoint Meta File Format

Checkpoint meta files contain the following columns:

| Column | Name | Type | Description |
|--------|------|------|-------------|
| 0 | start_ts | types.TS | Start timestamp |
| 1 | end_ts | types.TS | End timestamp |
| 2 | meta_location | objectio.Location | Meta location |
| 3 | entry_type | bool | Entry type (true=incremental, false=global) |
| 4 | version | uint32 | Version number |
| 5 | all_locations | objectio.Location | All locations |
| 6 | checkpoint_lsn | uint64 | Checkpoint LSN |
| 7 | truncate_lsn | uint64 | Truncate LSN |
| 8 | type | int8 | Entry type (0=ET_Global, 1=ET_Incremental, 2=ET_Compacted, 3=ET_Backup) |

## Common Use Cases

### 1. Inspect Checkpoint File Contents

When you need to see what's in a checkpoint file:

```bash
ckp-tool read -file /path/to/meta_123_456.ckp
```

### 2. Remove a Corrupted Entry

If you know the index of a corrupted entry:

```bash
# First, read to find the problematic row
ckp-tool read -file /path/to/meta_123_456.ckp

# Then delete it by index
ckp-tool delete -file /path/to/meta_123_456.ckp -index 2
```

### 3. Remove Entry by Timestamp

If you know the end_ts value of the entry to remove:

```bash
ckp-tool delete -file /path/to/meta_123_456.ckp -end-ts '1766148146913695795-1'
```

### 4. Create a Backup Before Modification

Always create a backup before modifying checkpoint files:

```bash
# Copy the original file
cp /path/to/meta_123_456.ckp /path/to/meta_123_456.ckp.backup

# Delete and save to new file
ckp-tool delete -file /path/to/meta_123_456.ckp -index 0 -output /path/to/meta_123_456_fixed.ckp
```

## Error Handling

The tool provides clear error messages for common issues:

- **File not found**: Check that the file path is correct
- **Invalid row index**: Ensure the index is within the valid range [0, row_count)
- **Row not found**: When using `-end-ts`, ensure the value matches exactly
- **File service errors**: Check file permissions and disk space

## Safety Considerations

⚠️ **Warning**: Modifying checkpoint meta files can affect database recovery and consistency. Always:

1. **Backup first**: Create a backup of the original file before making changes
2. **Verify**: Use the `read` command to verify file contents before and after modifications
3. **Test**: Test changes in a non-production environment first
4. **Understand**: Make sure you understand the implications of deleting checkpoint entries

## Troubleshooting

### File Cannot Be Read

If you encounter checksum validation errors:

```bash
# Use S3 mode to skip checksum validation
ckp-tool read -file /path/to/file.ckp -s3=true
```

### Row Not Found

When deleting by `end_ts`, ensure the value matches exactly. Use the `read` command to see the exact format:

```bash
# First, read to see the exact end_ts format
ckp-tool read -file /path/to/file.ckp

# Then use the exact value shown
ckp-tool delete -file /path/to/file.ckp -end-ts '1766148146913695795-1'
```

## Testing

Run the test suite:

```bash
cd cmd/ckp-tool
go test -v
```

## Implementation Details

The tool uses MatrixOne's fileservice and objectio packages to:
- Read checkpoint files using `ioutil.NewFileReader`
- Write checkpoint files using `objectio.NewObjectWriterSpecial` with `WriterCheckpoint` type
- Handle both S3FS (DISK backend) and LocalFS file services
- Manage memory pools for batch operations

## License

Copyright 2021 Matrix Origin. Licensed under the Apache License, Version 2.0.

