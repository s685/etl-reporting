#!/usr/bin/env bash
#â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Test harness for git_auto.sh
#  Usage: ./test_git_auto.sh [workflow_num] [iterations]
#  Example: ./test_git_auto.sh 2 2   # Run Option 2 (Stage, Commit & Push) twice
#â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_AUTO="${SCRIPT_DIR}/git_auto.sh"
WORKFLOW="${1:-2}"
ITERATIONS="${2:-2}"

run_option2() {
    local iter="$1"
    # Option 2 flow: 2, 1 (stage all), 1 (feat), (enter scope), desc, n (no body), y (commit), y (push or n for protected)
    # On main (protected): 2, 1, 1, , "test run $iter", n, y, y, y (SURE push)
    # + Enter to return to menu
    {
        echo "2"
        echo "1"
        echo "1"
        echo ""
        echo "automated test run $iter"
        echo "n"
        echo "y"
        echo "y"
        echo "y"
        echo ""
    } | bash "$GIT_AUTO" 2>&1
}

run_option6() {
    local iter="$1"
    {
        echo "6"
        echo "1"
        echo ""
    } | bash "$GIT_AUTO" 2>&1
}

run_option7() {
    {
        echo "7"
        echo "n"
        echo ""
    } | bash "$GIT_AUTO" 2>&1
}

run_option8() {
    {
        echo "8"
        echo "3"
        echo ""
    } | bash "$GIT_AUTO" 2>&1
}

run_option5_list() {
    {
        echo "5"
        echo "2"
        echo ""
    } | bash "$GIT_AUTO" 2>&1
}

run_quit() {
    echo "q" | bash "$GIT_AUTO" 2>&1
}

main() {
    if [[ ! -f "$GIT_AUTO" ]]; then
        echo "Error: git_auto.sh not found at $GIT_AUTO"
        exit 1
    fi

    echo "=== Testing workflow $WORKFLOW, $ITERATIONS iteration(s) ==="

    case "$WORKFLOW" in
        2)
            for i in $(seq 1 "$ITERATIONS"); do
                echo "--- Option 2 Run $i ---"
                run_option2 "$i" || true
            done
            run_quit || true
            ;;
        5)
            for i in $(seq 1 "$ITERATIONS"); do
                echo "--- Option 5 (List) Run $i ---"
                run_option5_list || true
            done
            run_quit || true
            ;;
        6)
            for i in $(seq 1 "$ITERATIONS"); do
                echo "--- Option 6 Run $i ---"
                run_option6 "$i" || true
            done
            run_quit || true
            ;;
        7)
            echo "--- Option 7 (decline config) ---"
            run_option7 || true
            run_quit || true
            ;;
        8)
            for i in $(seq 1 "$ITERATIONS"); do
                echo "--- Option 8 (prune) Run $i ---"
                run_option8 || true
            done
            run_quit || true
            ;;
        *)
            echo "Unknown workflow. Use 2, 5, 6, 7, or 8."
            exit 1
            ;;
    esac

    echo "=== Test complete ==="
}

main "$@"
