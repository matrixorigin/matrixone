#!/usr/bin/awk -f

# Report packages and test cases that started but did not emit a terminal
# go test -json event. Keep JSON string values escaped so arbitrary test names
# cannot inject extra diagnostic lines.

function json_string_field(line, name,    marker, start, rest, i, ch, escaped) {
    marker = "\"" name "\":\""
    start = index(line, marker)
    if (start == 0) {
        return ""
    }

    rest = substr(line, start + length(marker))
    escaped = 0
    for (i = 1; i <= length(rest); i++) {
        ch = substr(rest, i, 1)
        if (ch == "\"" && !escaped) {
            return substr(rest, 1, i - 1)
        }
        if (ch == "\\" && !escaped) {
            escaped = 1
        } else {
            escaped = 0
        }
    }
    return ""
}

{
    action = json_string_field($0, "Action")
    package_name = json_string_field($0, "Package")
    test_name = json_string_field($0, "Test")
    event_time = json_string_field($0, "Time")

    if (package_name == "" || action == "") {
        next
    }

    if (test_name == "") {
        if (action == "start") {
            active_packages[package_name] = event_time
        } else if (action == "pass" || action == "fail" || action == "skip") {
            delete active_packages[package_name]
        }
        next
    }

    key = package_name SUBSEP test_name
    if (action == "run" || action == "pause" || action == "cont") {
        if (!(key in active_cases)) {
            active_cases[key] = event_time
        }
        case_packages[key] = package_name
        case_names[key] = test_name
        case_actions[key] = action
    } else if (action == "pass" || action == "fail" || action == "skip") {
        delete active_cases[key]
        delete case_packages[key]
        delete case_names[key]
        delete case_actions[key]
    }
}

END {
    count = 0
    for (key in active_cases) {
        package_has_case[case_packages[key]] = 1
        printf "active UT case: package=%s test=%s state=%s started=%s\n", \
            case_packages[key], case_names[key], case_actions[key], active_cases[key]
        count++
    }
    for (package_name in active_packages) {
        if (!(package_name in package_has_case)) {
            printf "active UT package (no active case event): package=%s started=%s\n", \
                package_name, active_packages[package_name]
            count++
        }
    }
    if (count == 0) {
        print "no active or incomplete UT package/test case found"
    }
}
