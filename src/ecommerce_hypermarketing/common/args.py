import argparse


def parse_args(extra=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema-prefix', required=True)
    parser.add_argument('--mode', default='batch', choices=['batch', 'autoloader'])
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--stock-low-hrs', type=float, default=12.0)
    parser.add_argument('--min-push-roas', type=float, default=5.0)
    parser.add_argument('--min-push-cvr', type=float, default=4.5)
    if extra:
        for args, kwargs in extra:
            parser.add_argument(*args, **kwargs)
    return parser.parse_args()
