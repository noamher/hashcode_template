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


def solve(input_file, output_dir, checker=False):
    print("Parsing input file '{}'".format(input_file))
    inp = parse_input(input_file)

    res = actual_solve(inp)

    score = None
    if checker:
        score = check(res)
        print("Score for input file '{}' is {}".format(input_file, score))

    write_output(output_dir, input_file, res, score)


def check(result):
    # <insert score checking code...>
    assert 1 < 2
    return 42


def write_output(output_dir, input_file, result, score):
    output_file_name = output_dir / Path(input_file).with_suffix('.out').name
    if score is not None:
        file_name = '{}__{}.out'.format(Path(input_file).stem, score)
        output_file_name = output_dir / Path(input_file).with_name(file_name)

    print("Writing output to file '{}'".format(output_file_name))

    with Path(output_file_name).open('w') as f:
        # <insert code to write result to file ...>
        for l in result:
            f.write('{}\n'.format(l).decode('utf-8'))


def parse_input(input_file):
    inp = []

    with open(input_file, 'r') as f:
        for l in f:
            # <insert parsing code ...>
            splitted = l.split()
            line = splitted[0], float(splitted[1])
            inp.append(line)

    return inp


def persist(persist_alias, output_dir):
    persist_dir = _persist_dir(persist_alias)

    print("Copying solution package to persist dir")
    shutil.copytree(str(output_dir), str(persist_dir))
    return persist_dir


def _persist_dir(alias):
    persist_base_dir = Path(__file__).parent.parent / 'persist'
    persist_base_dir.mkdir(parents=True, exist_ok=True)
    persist_dir = persist_base_dir / '{}_{}'.format(datetime.now().strftime('%H%M%S'), alias)
    return persist_dir


def main(args):
    output_dir = Path(__file__).parent.parent / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.persist:
        persist(args.persist, output_dir)
        return

    # erase previous results
    for f in output_dir.glob('./*.out'):
        f.unlink()

    jobs = [delayed(solve)(input_file, output_dir, args.checker) for input_file in args.input_files]
    cpus = -1 if args.parallel else 1
    Parallel(n_jobs=cpus)(jobs)


def get_parser():
    parser = argparse.ArgumentParser(description='Hash Code solver')
    parser.add_argument('input_files', type=str, nargs='+')
    parser.add_argument('--parallel', action='store_true')
    parser.add_argument('--checker', action='store_true')
    parser.add_argument('--persist', type=str, default=None,
                        help="Persist current code and outputs of last run and then exit.")
    return parser


if __name__ == '__main__':
    args = get_parser().parse_args()
    main(args)