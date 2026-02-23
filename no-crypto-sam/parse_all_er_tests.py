import re
from math import ceil, log
import os
from detect_failed_er_tests import detect_failed

class experimental_config:
    def __init__(self, n, d, pointer, move_semantics, bs, algo_allocs, algo_reads, high_level_allocs, high_level_reads):
        self.n = n
        self.d = d
        self.pointer = pointer
        self.move_semantics = move_semantics
        self.bs = bs
        self.algo_allocs = algo_allocs
        self.algo_reads = algo_reads
        self.high_level_allocs = high_level_allocs
        self.high_level_reads = high_level_reads
        self.total_allocs = 0
        
        self.algo_allocs['build'] = 0
        self.algo_reads['build'] = 0

        # average the building stats for each algorithm
        different_builds = 0
        for function in list(self.algo_allocs.keys()):
            if 'build' != function and 'build' in function:
                different_builds += 1
                self.algo_allocs['build'] += self.algo_allocs[function]
                self.algo_reads['build'] += self.algo_reads[function]

                del self.algo_allocs[function]
                del self.algo_reads[function]

        self.algo_allocs['build'] //= different_builds
        self.algo_reads['build'] //= different_builds

        if pointer == "recursive":
            for allocs in self.algo_allocs.values():
                self.total_allocs += allocs
            
            for function in list(self.algo_allocs.keys()):
                if "SmartAVLTree" in high_level_allocs[function]:
                    if n == 2048 and d == 20 and bs == 64 and pointer == 'recursive':
                        print("??", function, self.high_level_allocs[function]["SmartAVLTree"])
                    self.total_allocs -= self.high_level_allocs[function]["SmartAVLTree"]

            recursion_base = self.bs/8
            self.recursion_levels = log(self.total_allocs, recursion_base)

            if n == 2048 and d == 20 and bs == 64 and pointer == 'recursive':
                print("total", self.total_allocs, pointer)
                for k, v in algo_allocs.items():
                    print(k, v)
                print("RL", self.recursion_levels)
            
            if self.bs == 4096:
                self.recursion_levels = ceil(max(self.recursion_levels-1, 1))
            else:
                self.recursion_levels = ceil(max(self.recursion_levels-4, 1))
  
            for function in list(self.algo_reads.keys()):
                if "SmartAVLTree" in high_level_reads[function]:
                    self.algo_reads[function] = (self.algo_reads[function] - self.high_level_reads[function]["SmartAVLTree"])*self.recursion_levels + self.high_level_reads[function]["SmartAVLTree"]
                    if n == 2048 and d == 20 and bs == 64 and pointer == 'recursive':
                        print("AVL", function, self.algo_reads[function], self.high_level_reads[function]["SmartAVLTree"])    
                        print("AFTER", self.algo_reads[function])
                else:
                    self.algo_reads[function] *= (self.recursion_levels)

            print("recursion levels (recursive):", self.recursion_levels, self.bs, self.total_allocs, self.d, self.n)
            
    def __hash__(self):
        return hash((self.n, self.d, self.pointer, self.move_semantics, self.bs))

    def __eq__(self, other):
        return (self.n, self.d, self.pointer, self.move_semantics, self.bs) == (other.n, other.d, other.pointer, other.move_semantics, other.bs)

class high_level_config:
    def __init__(self, n, d, pointer, move_semantics, bs, high_level_allocs, high_level_reads):
        self.n = n
        self.d = d
        self.pointer = pointer
        self.move_semantics = move_semantics
        self.bs = bs
        self.high_level_allocs = high_level_allocs
        self.high_level_reads = high_level_reads

        self.high_level_allocs['build'] = dict()
        self.high_level_reads['build'] = dict()

        # average the building stats for each algorithm
        different_builds = 0
        for function in list(self.high_level_allocs.keys()):
            if 'build' != function and 'build' in function:
                different_builds += 1
                for structure in self.high_level_allocs[function]:
                    if structure not in self.high_level_allocs['build']:
                        self.high_level_allocs['build'][structure] = self.high_level_allocs[function][structure]
                        self.high_level_reads['build'][structure] = self.high_level_reads[function][structure]
                    else:
                        self.high_level_allocs['build'][structure] += self.high_level_allocs[function][structure]
                        self.high_level_reads['build'][structure] += self.high_level_reads[function][structure]

                del self.high_level_allocs[function]
                del self.high_level_reads[function]

        for structure in self.high_level_allocs['build'].keys():
            self.high_level_allocs['build'][structure] //= different_builds
            self.high_level_reads['build'][structure] //= different_builds

    def __lt__(self, other):
        return self.n < other.n

def get_parameters():
    # read parameters.log file to parse parameters
    f = open('parameters.log')
    log_text = f.read()
    f.close()

    powers = []
    degrees = []
    block_sizes = []
    functions = dict()
    prime_graph = None
    make_copies = None

    all_functions = {
        "bfs" : "bfs",
        "dfs" : "dfs",
        "dijkstra" : "dijkstra",
        "prim" : "prim",
        "pr" : "pagerank", 
        "cd" : "contact discovery", 
        "dtc" : "directed triangle count", 
        "rw" : "random walk"
    }

    lines = log_text.strip().splitlines()
    for line in lines:
        if line.startswith("Powers: "):
            split_pows = line[8:].split(", ")
            for p in split_pows:
                powers.append(int(p))
        
        elif line.startswith("Degrees: "):
            split_degs = line[9:].split(", ")
            for d in split_degs:
                degrees.append(int(d))

        elif line.startswith("Block Sizes: "):
            split_blocks = line[13:].split(", ")
            for bs in split_blocks:
                block_sizes.append(int(bs))

        elif line.startswith("Pointers: "):
            pointers = line[10:].split(", ")

        elif line.startswith("Functions: "):
            split_fns = line[11:].split(", ")
            for fn in split_fns:
                functions[fn] = all_functions[fn]

        elif line.startswith("Prime Graph: "):
            prime_graph = True if 'True' in line else False

        elif line.startswith("Make Copies: "):
            make_copies = True if 'True' in line else False

    return powers, degrees, block_sizes, pointers, functions, prime_graph, make_copies

def parse_log(powers, degrees, block_sizes, pointers, functions, prime_graph, make_copies):
    files = set(os.listdir('logs'))
    failed = detect_failed(show_output=False)
    files = files - failed

    for d in degrees:
        for bs in block_sizes:
            experimental_results = []
            high_level_results = []
            algo_stats = {}
            high_level_stats = {}
            for p in powers:
                n = 2 ** p
                for pt in pointers:
                    if pt == 'original':
                        move_semantics_plural = ['True', 'False']
                    else:
                        move_semantics_plural = ['True']

                    for move_semantics in move_semantics_plural:
                        graph = f"ER_n{n}_d{d}_{pt}_semantics{move_semantics}_bs{bs}"
                                                
                        all_tests_present = True
                        for fn in functions:
                            if graph + f'_{fn}_prime{prime_graph}_copies{make_copies}.log' not in files:
                                all_tests_present = False

                        if not all_tests_present:
                            continue
                    
                        processed_high_level_build_ops = False
                        for fn in functions:
                            function = functions[fn]
                            file = graph + f'_{fn}_prime{prime_graph}_copies{make_copies}.log'
                            
                            if file in files:
                                files.remove(file)
                            else:
                                continue

                            f = open('logs/' + file)
                            log_text = f.read()
                            f.close()

                            lines = log_text.strip().splitlines()
                            i = 0
                            processed_high_level_build_ops = False
                            while i < len(lines):
                                line = lines[i]
                                i += 1

                                if "Skipping test: p=" in line:
                                    break

                                if "--------------------------------------------------------------" in line:
                                    processed_high_level_build_ops = True

                                elif line.startswith("----- Running test: "):
                                    if graph not in algo_stats:
                                        algo_stats[graph] = dict()
                                        high_level_stats[graph] = dict()
                                
                                elif line.startswith("Post build stats"):
                                    algo_stats[graph][f"build {function}"] = tuple(map(int, re.findall(r'\d+', line)))
                                    high_level_stats[graph][function] = dict()
                                    high_level_stats[graph][f"build {function}"] = dict()
                                
                                elif line.startswith("Post algorithm"):
                                    if function in line:
                                        algo_stats[graph][function] = tuple(map(int, re.findall(r'\d+', line)))
                                        high_level_stats[graph][function] = dict()

                                elif line.startswith("High level operations: "):
                                    if processed_high_level_build_ops:
                                        structure = line[23:].split(" ")[0]
                                        high_level_stats[graph][function][structure] = tuple(map(int, re.findall(r'\d+', line)))
                                    else:
                                        structure = line[23:].split(" ")[0]
                                        high_level_stats[graph][f"build {function}"][structure] = tuple(map(int, re.findall(r'\d+', line)))

            all_high_level_allocs = dict()
            all_high_level_reads = dict()

            for graph in high_level_stats.keys():
                high_level_allocs = dict()
                high_level_reads = dict()

                for function in functions.values():
                    high_level_allocs[f'build {function}'] = dict()
                    high_level_reads[f'build {function}'] = dict()
                    high_level_allocs[function] = dict()
                    high_level_reads[function] = dict()

                    for structure in high_level_stats[graph][function].keys():
                        build_structure_dict = high_level_stats[graph][f'build {function}'][structure]
                        function_structure_dict = high_level_stats[graph][function][structure]

                        high_level_allocs[f'build {function}'][structure] = build_structure_dict[0]
                        high_level_reads[f'build {function}'][structure] = build_structure_dict[1]
                        high_level_allocs[function][structure] = function_structure_dict[0] - build_structure_dict[0]
                        high_level_reads[function][structure] = function_structure_dict[1] - build_structure_dict[1]

                all_high_level_allocs[graph] = high_level_allocs
                all_high_level_reads[graph] = high_level_reads
                graph_namesplit = graph.split('_')
                move_semantics = True if 'True' in graph_namesplit[4] else False
                new_result = high_level_config(int(graph_namesplit[1][1:]), int(graph_namesplit[2][1:]), graph_namesplit[3], move_semantics, int(graph_namesplit[5][2:]), high_level_allocs, high_level_reads)
                high_level_results.append(new_result)

            generate_tables(d, bs, high_level_results, functions, powers, 'tables')

            # generate pgf plots
            for graph in algo_stats.keys():
                high_level_allocs = all_high_level_allocs[graph]
                high_level_reads = all_high_level_reads[graph]
                algo_allocs = dict()
                algo_reads = dict()

                for function in functions.values():
                    algo_allocs[f'build {function}'] = algo_stats[graph][f'build {function}'][0]
                    algo_reads[f'build {function}'] = algo_stats[graph][f'build {function}'][1]
                    algo_allocs[function] = algo_stats[graph][function][0] - algo_allocs[f'build {function}']
                    algo_reads[function] = algo_stats[graph][function][1] - algo_reads[f'build {function}']

                graph_namesplit = graph.split('_')
                move_semantics = True if 'True' in graph_namesplit[4] else False
                new_result = experimental_config(int(graph_namesplit[1][1:]), int(graph_namesplit[2][1:]), graph_namesplit[3], move_semantics, int(graph_namesplit[5][2:]), algo_allocs, algo_reads, high_level_allocs, high_level_reads)
                experimental_results.append(new_result)

            generate_plots(d, bs, experimental_results, functions, 'plots')

def generate_tables(d, bs, high_level_results, functions, powers, directory=None):
    damping_factor = 0.9
    walk_length = 50

    i = 0
    while 2 ** powers[i] < d:
        i += 1

    used_powers = powers[i:]

    functions['build'] = 'build'
    for function in functions.values():
        file = f"high_level_ops_table_{function}_reads_d"+str(d)+"_bs"+str(bs)+".tex"
        if type(directory) is str:
            if not os.path.exists(f'{directory}'):
                os.mkdir(f'{directory}')
            file = directory + '/' + file

        pointers_to_structures = dict()
        pointer_labels = set()

        for result in high_level_results:
            if result.d == d:
                pointer_label = str(result.pointer)+'-'+str(result.move_semantics)

                if pointer_label not in pointers_to_structures:
                    pointers_to_structures[pointer_label] = set()

                for structure in result.high_level_reads[function].keys():
                    if structure == "AddressQueue" and "original" not in pointer_label:
                        continue
                    pointers_to_structures[pointer_label].add(structure)

        with open(file, "w") as table_file:
            for pointer_label in pointers_to_structures.keys():
                table_file.write("\\begin{table}[]\n")
                table_file.write("\\centering\n")
                table_file.write("\\begin{tabular}{l|")
                column_line = "c|"
                powers_line = "$log_2(n)$ "
                for i in range(len(used_powers)):
                    column_line += "c|"
                    powers_line += f"& {used_powers[i]} "
                table_file.write(column_line + "}\n")
                table_file.write(powers_line + "\\\\ \\hline\n")

                high_level_results_matching = [result for result in high_level_results if (result.pointer + "-" + str(result.move_semantics) == pointer_label)]
                high_level_results_matching.sort()

                structures = [structure for structure in pointers_to_structures[pointer_label]]
                structures.sort()
                for structure in structures:
                    line = f"{structure} "
                    n = 2 ** used_powers[0]
                    i = 0
                    while n < 2 ** (len(used_powers) + 1):
                        if i < len(high_level_results_matching):
                            result = high_level_results_matching[i]                            
                            
                            while result.n > n:
                                line += "& 0 "
                                n *= 2

                            if (result.n == n):
                                line += f"& {result.high_level_reads[function][structure]} "

                                n *= 2
                            
                            i += 1
                        else:
                            line += "& 0 "
                            n *= 2

                    table_file.write(f"{line}\\\\ \\hline\n")
                        
                table_file.write("\\end{tabular}\n")
                fn = function

                caption = "High level operations for "
                match function:
                    case 'build':
                        caption = r"Building with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'bfs':
                        caption = r"BFS with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'dfs':
                        caption = r"DFS with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'dijkstra':
                        caption = r"Dijkstra with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'prim':
                        caption = r"Prim with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'random walk':
                        caption = r"Random Walk of length $" + f"{walk_length}" + r"$ with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'contact discovery':
                        caption = r"Contact Discovery with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'directed triangle count':
                        caption = r"Directed Triangle Count with pointer architecture " + f"{pointer_label}" + r", $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                    case 'pagerank':
                        caption = r"PageRank of length $" + f"{walk_length}" + r"$ with pointer architecture " + f"{pointer_label}" + r", damping factor $" + f"{damping_factor}" + r"$, $d\in\{" + f"{d}" + r"\}$, and block size $" + f"{bs}" + r"$"
                            
                table_file.write("\\caption{" + caption + "}\n")
                table_file.write("\\label{tab:placeholder}\n")
                table_file.write("\\end{table}\n\n")

    del functions['build'] 

def generate_plots(d, bs, experimental_results, functions, directory=None):
    functions['build'] = 'build'
    for function in functions.values():
        file = f"log2n_vs_{function}_reads_d"+str(d)+"_bs"+str(bs)+".pgf"
        if type(directory) is str:
            if not os.path.exists(f'{directory}'):
                os.mkdir(f'{directory}')
            file = directory + '/' + file
        with open(file, "w") as pgf_file:
            pgf_file.write("\\begin{tikzpicture}\n")
            pgf_file.write("\\begin{axis}[\n")
            pgf_file.write("xlabel={$\\log_2 n$}, ylabel={$\\log_2 (\\text{Reads})$},\n")
            pgf_file.write("width=10cm, height=8cm,\n")
            pgf_file.write("grid=major,\n")
            # Collect data by pointer type
            pointers = set((str(result.pointer)+'-'+str(result.move_semantics)) for result in experimental_results if result.d == d)
            pointers = sorted(list(pointers))
            legend_string = "legend entries={"+", ".join(pointers)+"},\n"
            pgf_file.write(legend_string)
            style_string = "legend style={at={(1.03,0.5)}, anchor=west},"
            pgf_file.write(style_string)
            pgf_file.write("\n]\n")
            for pointer in pointers:
                pgf_file.write("\\addplot+[only marks] coordinates {\n")
                for result in experimental_results:
                    if result.d == d and result.bs == bs and result.pointer == pointer.split('-')[0] and str(result.move_semantics) == pointer.split('-')[1]:
                        log2n = log(result.n, 2)
                        algo_reads = result.algo_reads[function]
                        if result.n == 2048 and d == 20 and bs == 64 and result.pointer == 'recursive':
                            print("HEY THERE", function, algo_reads)
                        reads = round(log(algo_reads, 2), 2)
                        pgf_file.write(f"({log2n}, {reads})\n")
                pgf_file.write("};\n")
            pgf_file.write("\\end{axis}\n")
            pgf_file.write("\\end{tikzpicture}\n")

    del functions['build'] 

# Example usage:
def parse_all_er_tests():
    assert os.path.exists('logs')
    powers, degrees, block_sizes, pointers, functions, prime_graph, make_copies = get_parameters()
    parse_log(powers, degrees, block_sizes, pointers, functions, prime_graph, make_copies)
    generate_figures(functions, block_sizes, degrees, prime_graph, make_copies)
    generate_main_table()

def generate_figures(functions, block_sizes, degrees, prime_graph, make_copies):
    files = set(os.listdir('plots'))

    damping_factor = 0.9
    walk_length = 50

    functions['build'] = 'build'
    with open('figures.tex', 'w') as f:
        for function in functions.values():
            for bs in block_sizes:
                f.write('\n' + r"\begin{figure}[ht]" + '\n')
                f.write(r"    \centering" + '\n')
                deg = ""
                for i in range(len(degrees)):
                    d = degrees[i]
                    f.write('\n')
                    f.write(r"    \begin{subfigure}{0.75\textwidth}" + '\n')
                    f.write(r"        \centering" + '\n')
                    f.write(r"        \resizebox{\linewidth}{!}{\input{Figures/log2n_vs_" + function + r"_reads_d" + f"{d}" + r"_bs" + f"{bs}" + r".pgf}}" + '\n')
                    f.write(r"        \caption{$d=" + f"{d}" + r"$}" + '\n')
                    f.write(r"    \end{subfigure}" + '\n')
                    
                    if i == len(degrees) - 1:
                        deg += f"{d}"
                        caption = ""
                        match function:
                            case 'build':
                                caption = r"Building with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'bfs':
                                caption = r"BFS with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'dfs':
                                caption = r"DFS with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'dijkstra':
                                caption = r"Dijkstra with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'prim':
                                caption = r"Prim with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'random walk':
                                caption = r"Random Walk of length $" + f"{walk_length}" + r"$ with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'contact discovery':
                                caption = r"Contact Discovery with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'directed triangle count':
                                caption = r"Directed Triangle Count with $d\in\{" + f"{deg}" + r"\}$ and block size $" + f"{bs}" + r"$"
                            case 'pagerank':
                                caption = r"PageRank of length $" + f"{walk_length}" + r"$ with damping factor $" + f"{damping_factor}" + r"$, $d\in\{" + f"{deg}" + r"\}$, and block size $" + f"{bs}" + r"$"
                            
                        caption += f". Done with priming "
                        if prime_graph:
                            caption += "enabled "
                        else:
                            caption += "disabled "
                        caption += "and smart copying "
                        if make_copies:
                            caption += "enabled."
                        else:
                            caption += "disabled."

                        f.write('\n' + r"    \caption{" + caption + r"}" + '\n\n')
                        f.write(r"\end{figure}" + '\n')
                    else:
                        f.write(r"    \hfill" + '\n')
                        deg += f"{d}, "

    del functions['build']

def generate_main_table():
    files = os.listdir('tables')
    files.sort()
    with open("tables.tex", "w") as f:
        for file in files:
            f.write(r"\input{tables/" + file.split(".")[0] + "}\n")

if __name__ == "__main__":
    parse_all_er_tests()
