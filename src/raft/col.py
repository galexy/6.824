import re
import sys

from rich.columns import Columns
from rich.console import Console
from rich.highlighter import Highlighter
import argparse


# class KeywordHighlighter(Highlighter):
#     def __init__(self, pattern):
#         self.pattern = pattern
#
#     def highlight(self, text):
#         for m in re.finditer(self.pattern, text._text[0]):
#             index = m.start()
#             text.stylize(f"magenta", index, index+len(self.pattern))


def process_lines(input, column_patterns, skip, keywords):
    console = Console(force_terminal=True)
    width = console.size.width
    n_columns = len(column_patterns) + 1
    col_width = int(width / n_columns)

    with console.pager(styles=True):
#         highlighter = KeywordHighlighter(keywords[0])

        for line in input:
            parts = line.strip().split(" ")
            prefix, message = " ".join(parts[:skip]), " ".join(parts[skip:])

            cols = ["" for _ in range(n_columns)]

            matched = False
            cols[0] = prefix
            for i, pattern in enumerate(column_patterns):
                if pattern in message:
                    matched = True
                    cols[i+1] = message
            if not matched:
                cols[0] = cols[0] + message

#             cols = [highlighter(col) for col in cols]
            console.print(Columns(cols, width=col_width-1, equal=False, expand=True))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        prog = 'Log Analyzer',
                        description = 'Dynamically format logs for analysis')

    parser.add_argument('-c', '--col', nargs="*")
    parser.add_argument('-s', '--skip', type=int, default=0)
    parser.add_argument('-k', '--keywords', nargs="*")
    args = parser.parse_args()

    process_lines(sys.stdin, args.col, args.skip, args.keywords)