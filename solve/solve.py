from __future__ import print_function
import argparse
import shutil
from datetime import datetime

from joblib import Parallel, delayed
from pathlib2 import Path


def actual_solve(inp):
    # <insert solving code ...>
    import time
    time.sleep(1)
    res = [l[1] for l in inp]

    return res


def solve(input_file, persist_dir=None):
    print("Parsing input file '{}'".format(input_file))
    inp = parse_input(input_file)

    res = actual_solve(inp)

    output_file = Path(input_file).with_suffix('.out')
    print("Writing output to file '{}'".format(output_file))
    write_output(output_file, res, persist_dir)


def write_output(output_file, result, persist_dir):
    with output_file.open('w') as f:
        # <insert code to write result to file ...>
        for l in result:
            f.write('{}\n'.format(l).decode('utf-8'))

    if persist_dir:
        shutil.copy(str(output_file), str(persist_dir))


def parse_input(input_file):
    inp = []

    with open(input_file, 'r') as f:
        for l in f:
            # <insert parsing code ...>
            splitted = l.split()
            line = splitted[0], float(splitted[1])
            inp.append(line)

    return inp


def main(args):
    persist_dir = None
    if args.persist:
        solution_dir = Path(__file__).parent
        persist_base_dir = solution_dir.parent / 'persist'
        persist_base_dir.mkdir(parents=True, exist_ok=True)
        persist_dir = persist_base_dir / '{}_{}'.format(datetime.now().strftime('%H%M%S'), args.persist)

        print("Copying solution package to persist dir")
        shutil.copytree(str(solution_dir), str(persist_dir))


    jobs = [delayed(solve)(input_file, persist_dir) for input_file in args.input_files]
    cpus = -1 if args.parallel else 1
    Parallel(n_jobs=cpus)(jobs)


def get_parser():
    parser = argparse.ArgumentParser(description='Hash Code solver')
    parser.add_argument('input_files', type=str, nargs='+')
    parser.add_argument('--parallel', action='store_true')
    parser.add_argument('--persist', type=str, default=None)
    return parser


if __name__ == '__main__':
    args = get_parser().parse_args()
    main(args)