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


def solve(input_file, persist_dir=None, checker=False):
    print("Parsing input file '{}'".format(input_file))
    inp = parse_input(input_file)

    res = actual_solve(inp)

    score = None
    if checker:
        score = check(res)
        print("Score for input file '{}' is {}".format(input_file, score))

    output_file = Path(input_file).with_suffix('.out')
    print("Writing output to file '{}'".format(output_file))
    write_output(output_file, res, persist_dir, score)


def check(result):
    # <insert score checking code...>
    assert 1 < 2
    return 42


def write_output(output_file, result, persist_dir, score):
    with output_file.open('w') as f:
        # <insert code to write result to file ...>
        for l in result:
            f.write('{}\n'.format(l).decode('utf-8'))

    if persist_dir:
        persist_output_file = persist_dir / '{}{}{}'.format(
            output_file.stem,
            '__{}'.format(score) if score else '',
            output_file.suffix,
        )
        shutil.copy(str(output_file), str(persist_output_file))


def parse_input(input_file):
    inp = []

    with open(input_file, 'r') as f:
        for l in f:
            # <insert parsing code ...>
            splitted = l.split()
            line = splitted[0], float(splitted[1])
            inp.append(line)

    return inp


def persist_last(persist_alias):
    print("Persisting last results and current code")

    solution_dir, persist_dir = _persist_dirs(persist_alias)
    output_dir = solution_dir.parent

    print("Copying solution package to persist dir")
    shutil.copytree(str(solution_dir), str(persist_dir))

    print("Copying last outputs to persist dir")
    for src in output_dir.glob('*.out'):
        shutil.copy(str(src), str(persist_dir))


def persist(persist_alias):
    solution_dir, persist_dir = _persist_dirs(persist_alias)

    print("Copying solution package to persist dir")
    shutil.copytree(str(solution_dir), str(persist_dir))
    return persist_dir


def _persist_dirs(alias):
    solution_dir = Path(__file__).parent
    persist_base_dir = solution_dir.parent / 'persist'
    persist_base_dir.mkdir(parents=True, exist_ok=True)
    persist_dir = persist_base_dir / '{}_{}'.format(datetime.now().strftime('%H%M%S'), alias)
    return solution_dir, persist_dir


def main(args):
    if args.persist_last:
        persist_last(args.persist_last)
        return

    persist_dir = None
    if args.persist:
        persist_dir = persist(args.persist)

    jobs = [delayed(solve)(input_file, persist_dir, args.checker) for input_file in args.input_files]
    cpus = -1 if args.parallel else 1
    Parallel(n_jobs=cpus)(jobs)


def get_parser():
    parser = argparse.ArgumentParser(description='Hash Code solver')
    parser.add_argument('input_files', type=str, nargs='+')
    parser.add_argument('--parallel', action='store_true')
    parser.add_argument('--checker', action='store_true')
    parser.add_argument('--persist', type=str, default=None)
    parser.add_argument('--persist-last', type=str, default=None,
                        help="Persist current code and outputs of last run and then exit.")
    return parser


if __name__ == '__main__':
    args = get_parser().parse_args()
    main(args)