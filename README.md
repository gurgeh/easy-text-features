# Easy Text Features

The purpose of these scripts is to help convert text (in any language) to numeric features. These features can then be used, together with standard numeric features, by predictive modelling software.

## Installation
The scripts can be downloaded from Github, using the green "Download"-button. They need [Python 2.7](https://www.python.org/downloads/) installed and should work on any OS. There are no other dependencies.

## Usage
`chargram.py` converts a CSV file with text in the *first* column to a CSV file with many numeric feature columns instead. These features are based on a running window of the actual characters/letters in the text, as opposed to the words. This should work better than a word base approach for most languages and purposes, but YMMV.

The CSV filename can be specified as the first program argument on the command line. If no command line argument is given, the default name `text.csv` is used.

`wordgram.py` works exactly the same as `chargram.py`, but the features are based on words instead.

## License
These utilities are released under the Apache 2.0 license. This means that they are free to use, also for commercial purposes. Read the LICENSE file for details.

## Want more?
To do more with your text in a multitude of languages, for example things like:
- Automatic tagging (topic modelling)
- Categorizing based on user defined examples
- Clustering
- Better predictive modelling based on text,
contact <info@crawlica.com>.

![Analyze ALL the things](https://cdn.meme.am/instances/500x/69257243.jpg)
