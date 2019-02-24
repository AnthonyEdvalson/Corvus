import os
import subprocess
import sys
import time
import atexit

corvus_dir = os.path.dirname(os.path.realpath(__file__))
programs = {}



def main():
    args = sys.argv[1:]
    if len(args) == 1:
        # running locally
        vert_addr = setup_vertex()
        run(args[0], vert_addr)

    elif len(args) == 2:
        # connecting to vertex
        run(args[0], args[1])


def setup_vertex():
    vertex = run_program("vertex", ['python3.7', corvus_dir + "/vertex/main.py"], True)
    line = vertex.stdout.readline()
    vert_addr = str(line[:-1].decode())

    if not vert_addr:
        exit(0)

    return vert_addr


def run(node_config_path, vert_addr):
    run_program("node", ['python3.7', corvus_dir + "/node/node.py", node_config_path, vert_addr])

    while True:
        time.sleep(0.2)
        for name, program in programs.items():
            if False:  # program.poll() is None:
                print(name + " has ended")
                return


def run_program(name, command, pipe=False):
    print("Running: " + " ".join(command))

    if pipe:
        program = subprocess.Popen(command, stdout=subprocess.PIPE)
    else:
        program = subprocess.Popen(command)

    programs[name] = program

    return program


def cleanup():
    print("Closing subprocesses")
    for program in programs.values():
        program.terminate()
        program.wait()


if __name__ == '__main__':
    atexit.register(cleanup)
    main()
