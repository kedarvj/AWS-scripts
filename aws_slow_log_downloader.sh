#!/usr/bin/env bash
# Author: Kedar Vaijanapurkar
# Script: download_rds_slow_logs.sh
# Purpose: Download slow query logs from AWS RDS
# Usage: ./download_rds_slow_logs.sh [OPTIONS]
#
# Prerequisites:
#   - AWS CLI v2, configured and authenticated (aws configure / sso / instance profile)
#   - Bash 4.0+
#
# Options:
#   -d DATE           Start date (YYYYMMDD): download from this date to latest on RDS
#   -i INSTANCE_ID    RDS instance identifier (required, or set RDS_INSTANCE_ID)
#   -o OUTPUT_DIR     Base output directory (default: SLOW_LOG_HOME or ./slowquery)
#   -m MAX_RETRIES    AWS max retry attempts (default: AWS_MAX_ATTEMPTS or 10)
#   -j JOBS           Parallel download jobs (default: 4)
#   -w SECONDS        Watch mode: poll for new logs every SECONDS (daemon mode)
#   -f                Force re-download even if files exist
#   -h                Show this help message
#
# Environment variables (used as defaults when flags are not provided):
#   RDS_INSTANCE_ID   - RDS instance identifier
#   SLOW_LOG_HOME     - Base directory for storing downloaded logs
#   AWS_MAX_ATTEMPTS  - Max AWS API retry attempts
#

set -euo pipefail

# ------------------------------------------------------------------------------
# Defaults (from environment or hardcoded fallbacks)
# ------------------------------------------------------------------------------
readonly SCRIPT_NAME="$(basename "$0")"

TARGET_DATE=""
USER_SET_DATE=false   # true when -d or positional date was given
INSTANCE_ID="${RDS_INSTANCE_ID:-}"
OUTPUT_DIR="${SLOW_LOG_HOME:-./slowquery}"
MAX_RETRIES="${AWS_MAX_ATTEMPTS:-10}"
PARALLEL_JOBS=4
WATCH_INTERVAL=0  # 0 = one-shot mode (no daemon)
FORCE_DOWNLOAD=false
SHUTDOWN_REQUESTED=false

# ------------------------------------------------------------------------------
# Logging helpers
# ------------------------------------------------------------------------------
log_info()  { echo "[INFO]  $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
log_warn()  { echo "[WARN]  $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# ------------------------------------------------------------------------------
# Usage
# ------------------------------------------------------------------------------
usage() {
    sed -n '2,/^$/s/^#//p' "$0" | sed 's/^ //'
    exit "${1:-0}"
}

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------
parse_args() {
    while getopts ":d:i:o:m:j:w:fh" opt; do
        case "$opt" in
            d) TARGET_DATE="$OPTARG" ;;
            i) INSTANCE_ID="$OPTARG" ;;
            o) OUTPUT_DIR="$OPTARG" ;;
            m) MAX_RETRIES="$OPTARG" ;;
            j) PARALLEL_JOBS="$OPTARG" ;;
            w) WATCH_INTERVAL="$OPTARG" ;;
            f) FORCE_DOWNLOAD=true ;;
            h) usage 0 ;;
            :) log_error "Option -$OPTARG requires an argument"; usage 1 ;;
            *) log_error "Unknown option: -$OPTARG"; usage 1 ;;
        esac
    done
    shift $((OPTIND - 1))

    # Positional argument as date (backward compat with original script)
    if [[ -z "$TARGET_DATE" && $# -gt 0 ]]; then
        TARGET_DATE="$1"
    fi

    if [[ -n "$TARGET_DATE" ]]; then
        USER_SET_DATE=true
    fi
}

# ------------------------------------------------------------------------------
# Validate inputs
# ------------------------------------------------------------------------------
validate() {
    # Validate date format (only when explicitly provided)
    if [[ "$USER_SET_DATE" == true ]]; then
        if ! [[ "$TARGET_DATE" =~ ^[0-9]{8}$ ]]; then
            log_error "Invalid date format: '$TARGET_DATE'. Expected YYYYMMDD."
            exit 1
        fi
    fi

    # Validate instance ID
    if [[ -z "$INSTANCE_ID" ]]; then
        log_error "RDS instance identifier is required. Use -i flag or set RDS_INSTANCE_ID."
        usage 1
    fi

    # Validate parallel jobs is a number
    if ! [[ "$PARALLEL_JOBS" =~ ^[0-9]+$ ]] || [[ "$PARALLEL_JOBS" -lt 1 ]]; then
        log_error "Parallel jobs (-j) must be a positive integer."
        exit 1
    fi

    # Validate watch interval
    if ! [[ "$WATCH_INTERVAL" =~ ^[0-9]+$ ]]; then
        log_error "Watch interval (-w) must be a non-negative integer (seconds)."
        exit 1
    fi
    if [[ "$WATCH_INTERVAL" -gt 0 && "$WATCH_INTERVAL" -lt 60 ]]; then
        log_warn "Watch interval ${WATCH_INTERVAL}s is very short. Consider 300+ to avoid API throttling."
    fi

    # Validate AWS CLI is available and configured
    if ! command -v aws &>/dev/null; then
        log_error "AWS CLI is not installed or not in PATH."
        exit 1
    fi

    if ! aws sts get-caller-identity &>/dev/null; then
        log_error "AWS CLI is not configured. Run 'aws configure' or 'aws sso login' first."
        exit 1
    fi
}

# ------------------------------------------------------------------------------
# Manifest helpers — track completed downloads for reliable resume
#
# Manifest format (one line per file):
#   <local_filename> <size_bytes> <sha256_first8>
# Stored at: $day_dir/.manifest
# ------------------------------------------------------------------------------
readonly MANIFEST_NAME=".manifest"

manifest_path() {
    echo "${1}/${MANIFEST_NAME}"
}

# Check if a log file is recorded as successfully downloaded in the manifest
manifest_has_file() {
    local manifest="$1"
    local filename="$2"
    [[ -f "$manifest" ]] && grep -q "^${filename} " "$manifest"
}

# Record a successfully downloaded file in the manifest
manifest_record() {
    local manifest="$1"
    local filepath="$2"

    local filename
    filename="$(basename "$filepath")"
    local size
    size="$(wc -c < "$filepath")"
    local checksum
    checksum="$(sha256sum "$filepath" | cut -c1-8)"

    # Remove any previous entry for this file, then append
    if [[ -f "$manifest" ]]; then
        sed -i "/^${filename} /d" "$manifest"
    fi
    echo "${filename} ${size} ${checksum}" >> "$manifest"
}

# Validate a local file against its manifest entry (size + checksum match)
manifest_validate_file() {
    local manifest="$1"
    local filepath="$2"

    local filename
    filename="$(basename "$filepath")"

    if [[ ! -f "$manifest" ]] || ! grep -q "^${filename} " "$manifest"; then
        return 1  # not in manifest
    fi

    local recorded
    recorded="$(grep "^${filename} " "$manifest")"
    local expected_size expected_checksum
    expected_size="$(echo "$recorded" | awk '{print $2}')"
    expected_checksum="$(echo "$recorded" | awk '{print $3}')"

    # Check file still exists and matches
    if [[ ! -f "$filepath" ]]; then
        return 1
    fi

    local actual_size
    actual_size="$(wc -c < "$filepath")"
    local actual_checksum
    actual_checksum="$(sha256sum "$filepath" | cut -c1-8)"

    [[ "$actual_size" == "$expected_size" && "$actual_checksum" == "$expected_checksum" ]]
}

# ------------------------------------------------------------------------------
# Download a single log file portion
# ------------------------------------------------------------------------------
download_single_log() {
    local log_file_name="$1"
    local output_file="$2"
    local manifest="$3"

    local filename
    filename="$(basename "$output_file")"

    # Resume logic: skip if manifest confirms a valid prior download
    if [[ "$FORCE_DOWNLOAD" == false ]]; then
        if manifest_validate_file "$manifest" "$output_file"; then
            log_info "Skipping (verified in manifest): $filename"
            echo "skipped"
            return 0
        elif [[ -f "$output_file" && -s "$output_file" ]]; then
            # File exists but isn't in manifest or failed validation — re-download
            log_warn "Re-downloading (not in manifest or checksum mismatch): $filename"
        fi
    fi

    local tmp_file="${output_file}.tmp"

    # Clean up any leftover .tmp from a previous crashed run
    rm -f "$tmp_file"

    if ! AWS_MAX_ATTEMPTS="$MAX_RETRIES" aws rds download-db-log-file-portion \
        --output text \
        --db-instance-identifier "$INSTANCE_ID" \
        --log-file-name "$log_file_name" \
        --starting-token 0 \
        > "$tmp_file" 2>/dev/null; then
        log_warn "Failed to download: $log_file_name"
        rm -f "$tmp_file"
        return 1
    fi

    # Only keep non-empty downloads
    if [[ -s "$tmp_file" ]]; then
        mv "$tmp_file" "$output_file"
        manifest_record "$manifest" "$output_file"
        log_info "Downloaded: $filename ($(wc -c < "$output_file") bytes)"
        echo "downloaded"
    else
        log_warn "Empty response from RDS: $log_file_name"
        rm -f "$tmp_file"
        echo "empty"
        return 0
    fi
}

# Export function and variables for use with xargs/parallel
export -f download_single_log log_info log_warn log_error \
         manifest_has_file manifest_record manifest_validate_file manifest_path

# ------------------------------------------------------------------------------
# RDS log discovery — single API call, no date assumption
#
# Returns all slow query log filenames available on the RDS instance.
# The caller decides which dates to process based on mode (auto / -d).
# ------------------------------------------------------------------------------
discover_all_slow_logs() {
    log_info "Querying RDS for available slow query logs ..."

    local raw_output
    raw_output=$(aws rds describe-db-log-files \
        --db-instance-identifier "$INSTANCE_ID" \
        --output text \
        --query "DescribeDBLogFiles[?contains(LogFileName, \`slowquery\`)].LogFileName" \
    ) || {
        log_error "Failed to list log files from RDS instance '$INSTANCE_ID'."
        return 1
    }

    # Output is tab-separated; emit one filename per line
    local file
    for file in $raw_output; do
        [[ -n "$file" ]] && echo "$file"
    done
}

# Extract sorted unique dates (YYYYMMDD) from slow query log filenames.
# Reads filenames from stdin, e.g. "slowquery/mysql-slowquery.log.2026-04-15.14"
extract_log_dates() {
    while IFS= read -r f; do
        if [[ "$f" =~ ([0-9]{4})-([0-9]{2})-([0-9]{2})\.[0-9]{2}$ ]]; then
            echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}${BASH_REMATCH[3]}"
        fi
    done | sort -u
}

# Filter log filenames for a specific date.
# The base log (slowquery/mysql-slowquery.log) is only included when
# include_base is "true" — callers pass true only for the latest date.
# Reads filenames from stdin.
filter_logs_for_date() {
    local date_str="$1"
    local include_base="$2"
    local date_hyphenated="${date_str:0:4}-${date_str:4:2}-${date_str:6:2}"

    while IFS= read -r f; do
        if [[ "$f" == *"$date_hyphenated"* ]]; then
            echo "$f"
        elif [[ "$include_base" == true && "$f" == "slowquery/mysql-slowquery.log" ]]; then
            echo "$f"
        fi
    done
}

# ------------------------------------------------------------------------------
# Download log files for a single date
# ------------------------------------------------------------------------------
download_for_date() {
    local target_date="$1"
    local log_files="$2"    # newline-separated list of RDS log filenames
    local day_dir="${OUTPUT_DIR}/${INSTANCE_ID}/${target_date}"
    local manifest="${day_dir}/${MANIFEST_NAME}"

    mkdir -p "$day_dir"

    # Report resume state
    if [[ -f "$manifest" ]]; then
        local prev_count
        prev_count="$(wc -l < "$manifest")"
        if [[ "$prev_count" -gt 0 ]]; then
            log_info "[$target_date] Manifest has $prev_count previously completed file(s)."
        fi
    fi

    # Build tasks file: log_file_name|local_output_path|manifest_path
    local tasks_file
    tasks_file=$(mktemp)

    local total=0
    while IFS= read -r log_file; do
        [[ -z "$log_file" ]] && continue
        local bname
        bname=$(echo "$log_file" | tr '/' '_')
        echo "${log_file}|${day_dir}/${bname}|${manifest}" >> "$tasks_file"
        ((total++)) || true
    done <<< "$log_files"

    if [[ "$total" -eq 0 ]]; then
        rm -f "$tasks_file"
        return 0
    fi

    log_info "[$target_date] Processing $total log file(s) ..."

    export INSTANCE_ID MAX_RETRIES FORCE_DOWNLOAD MANIFEST_NAME

    # Suppress stdout from download_single_log — the status echoes
    # (downloaded/skipped/empty) are informational only; log_info to stderr
    # provides all the detail needed.
    if command -v parallel &>/dev/null && [[ "$PARALLEL_JOBS" -gt 1 ]]; then
        parallel --colsep '\|' -j "$PARALLEL_JOBS" \
            download_single_log {1} {2} {3} < "$tasks_file" >/dev/null || true
    elif [[ "$PARALLEL_JOBS" -gt 1 ]]; then
        cat "$tasks_file" | xargs -P "$PARALLEL_JOBS" -I {} bash -c '
            IFS="|" read -r log_name out_path mfest <<< "{}"
            download_single_log "$log_name" "$out_path" "$mfest"
        ' >/dev/null || true
    else
        while IFS='|' read -r log_name out_path mfest; do
            download_single_log "$log_name" "$out_path" "$mfest" >/dev/null || true
        done < "$tasks_file"
    fi

    rm -f "$tasks_file"

    log_info "[$target_date] Complete: ${day_dir}/"
}

# ------------------------------------------------------------------------------
# Run one download cycle
#
# Makes a single API call to discover all slow query logs on RDS, extracts
# the dates present, and downloads the appropriate set:
#
#   -d DATE given:   all dates from DATE through the latest on RDS
#   no -d:           only the latest date on RDS (auto-detect)
#
# The base slow query log (slowquery/mysql-slowquery.log) is included only
# with the latest date to avoid duplicating it across date directories.
# ------------------------------------------------------------------------------
run_cycle() {
    # Single API call — get everything
    local all_logs
    all_logs=$(discover_all_slow_logs) || return 1

    if [[ -z "$all_logs" ]]; then
        log_warn "No slow query logs found on RDS."
        return 0
    fi

    # Extract dates that have hourly log parts
    local available_dates
    available_dates=$(echo "$all_logs" | extract_log_dates)

    if [[ -z "$available_dates" ]]; then
        # Only the base log exists — no dated hourly parts yet
        log_info "Only base slow query log available (no dated parts yet)."
        local effective_date
        if [[ "$USER_SET_DATE" == true ]]; then
            effective_date="$TARGET_DATE"
        else
            effective_date="$(date '+%Y%m%d')"
        fi
        local filtered
        filtered=$(echo "$all_logs" | filter_logs_for_date "$effective_date" true)
        if [[ -n "$filtered" ]]; then
            download_for_date "$effective_date" "$filtered"
        fi
        return 0
    fi

    local latest_date
    latest_date=$(echo "$available_dates" | tail -1)

    log_info "RDS has slow logs for date(s): $(echo "$available_dates" | tr '\n' ' '| sed 's/ $//')"
    log_info "Latest date on RDS: $latest_date"

    # Determine which dates to process
    local dates_to_process
    if [[ "$USER_SET_DATE" == true ]]; then
        # From the user-specified date through latest available
        dates_to_process=$(echo "$available_dates" | awk -v start="$TARGET_DATE" '$1 >= start')
    else
        # Auto-detect: latest date only
        dates_to_process="$latest_date"
    fi

    if [[ -z "$dates_to_process" ]]; then
        log_info "No log files found on RDS from $TARGET_DATE onward."
        return 0
    fi

    # Process each qualifying date
    while IFS= read -r date; do
        [[ -z "$date" ]] && continue

        # Include the base log only with the latest date
        local include_base=false
        if [[ "$date" == "$latest_date" ]]; then
            include_base=true
        fi

        local filtered
        filtered=$(echo "$all_logs" | filter_logs_for_date "$date" "$include_base")

        if [[ -n "$filtered" ]]; then
            download_for_date "$date" "$filtered"
        fi
    done <<< "$dates_to_process"
}

# ------------------------------------------------------------------------------
# Signal handling for graceful shutdown
# ------------------------------------------------------------------------------
handle_shutdown() {
    SHUTDOWN_REQUESTED=true
    log_info ""
    log_info "Shutdown signal received. Finishing current download cycle ..."
}

trap handle_shutdown SIGINT SIGTERM

# ------------------------------------------------------------------------------
# Write a PID file for daemon mode (allows external monitoring/kill)
# ------------------------------------------------------------------------------
write_pid_file() {
    local pid_file="${OUTPUT_DIR}/.download_rds_slow_logs.pid"
    mkdir -p "$OUTPUT_DIR"
    echo $$ > "$pid_file"
    log_info "PID file written: $pid_file ($$)"
}

remove_pid_file() {
    local pid_file="${OUTPUT_DIR}/.download_rds_slow_logs.pid"
    rm -f "$pid_file"
}

# ------------------------------------------------------------------------------
# Daemon / watch loop — polls for new logs at a regular interval
#
# Each cycle queries RDS directly for available log files. Date rollover is
# handled naturally: when RDS starts producing logs for a new date, the
# script picks them up automatically without relying on the server clock.
# ------------------------------------------------------------------------------
watch_loop() {
    local cycle=0

    write_pid_file
    trap remove_pid_file EXIT

    log_info "Entering watch mode (poll every ${WATCH_INTERVAL}s). Send SIGINT/SIGTERM to stop."

    while [[ "$SHUTDOWN_REQUESTED" == false ]]; do
        ((cycle++)) || true

        log_info "------ Cycle #${cycle} | $(date '+%Y-%m-%d %H:%M:%S') ------"

        run_cycle || log_warn "Cycle #${cycle} completed with errors."

        if [[ "$SHUTDOWN_REQUESTED" == true ]]; then
            break
        fi

        log_info "Next poll in ${WATCH_INTERVAL}s (Ctrl+C to stop) ..."

        # Interruptible sleep — check shutdown flag every second
        local waited=0
        while [[ "$waited" -lt "$WATCH_INTERVAL" && "$SHUTDOWN_REQUESTED" == false ]]; do
            sleep 1
            ((waited++)) || true
        done
    done

    log_info "=========================================="
    log_info " Daemon stopped after $cycle cycle(s)."
    log_info "=========================================="
}

# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------
main() {
    parse_args "$@"
    validate

    local mode="one-shot"
    if [[ "$WATCH_INTERVAL" -gt 0 ]]; then
        mode="watch (every ${WATCH_INTERVAL}s)"
    fi

    log_info "=========================================="
    log_info " RDS Slow Log Downloader"
    log_info "=========================================="
    log_info "Instance:   $INSTANCE_ID"
    if [[ "$USER_SET_DATE" == true ]]; then
        log_info "Start date: $TARGET_DATE (from here to latest on RDS)"
    else
        log_info "Date:       auto-detect from RDS"
    fi
    log_info "Output:     $OUTPUT_DIR"
    log_info "Parallel:   $PARALLEL_JOBS job(s)"
    log_info "Force:      $FORCE_DOWNLOAD"
    log_info "Mode:       $mode"
    log_info "=========================================="

    if [[ "$WATCH_INTERVAL" -gt 0 ]]; then
        watch_loop
    else
        run_cycle
    fi
}

main "$@"
