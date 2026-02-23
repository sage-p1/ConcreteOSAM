import os

def detect_failed(show_output=True):
    directory = "logs"

    skip_line = "Skipping test:"
    start_line = "----- Running test: "
    finish_line = "----- Finished test: "

    failed = set()

    functions = {
        "pr" : "pagerank", 
        "cd" : "contact discovery", 
        "dtc" : "directed triangle count", 
        "rw" : "random walk"
    }

    if os.path.exists(directory):
        logs = os.listdir(directory)
        for log in logs:
            test = log.split(".")[0].strip()
            f = open(f"{directory}/{log}", "r")
            text = f.read()

            fn = log.split("_")[6].split(".")[0]
            
            if not (skip_line in text or (start_line in text and finish_line in text and test in text and functions[fn] in text)):
                failed.add(log)

            f.close()

    if show_output:
        for fail in sorted(list(failed)):
            print(fail)

    else:
        return failed

if __name__ == "__main__":
    detect_failed(show_output=True)