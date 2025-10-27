# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------

project = 'MatrixOne Python SDK'
copyright = '2024, MatrixOne Team'
author = 'MatrixOne Team'
release = '1.0.0'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    # 'sphinx.ext.githubpages',  # Removed: not needed for ReadTheDocs deployment
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Suppress warnings from Python docstrings (these are code quality issues, not doc issues)
suppress_warnings = [
    'autosummary',
    'ref.python',  # Suppress duplicate object warnings
    'ref.doc',
    'ref',
    'app',
    'app.add_directive',
    'app.add_node',
]

# Ignore docstring formatting warnings from source code  
nitpicky = False

# Keep going on errors (don't stop build on warnings)
keep_going = True

# Autodoc configuration to reduce warnings
autodoc_default_flags = ['members', 'undoc-members']
autodoc_member_order = 'bysource'

# Napoleon settings to be more lenient
napoleon_use_rtype = False
napoleon_preprocess_types = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']  # Commented out since we don't have custom static files

# -- Extension configuration -------------------------------------------------

# -- Options for autodoc extension -------------------------------------------
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# -- Options for autosummary extension ---------------------------------------
autosummary_generate = True

# -- Options for napoleon extension ------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True

# -- Options for intersphinx extension ---------------------------------------
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/', None),
    # 'pymysql': ('https://pymysql.readthedocs.io/', None),  # Commented out - link not available
}

# -- Options for todo extension ----------------------------------------------
todo_include_todos = True

# -- Options for LaTeX output ------------------------------------------------
latex_engine = 'pdflatex'  # Use pdflatex for compatibility  
latex_elements = {
    'preamble': r'''
% Define text replacements for all emoji and special characters
\DeclareUnicodeCharacter{221A}{\ensuremath{\surd}}  % √
\DeclareUnicodeCharacter{2588}{\rule{0.6em}{1em}}  % █
\DeclareUnicodeCharacter{2705}{\textbf{[YES]}}     % ✅
\DeclareUnicodeCharacter{274C}{\textbf{[NO]}}      % ❌
\DeclareUnicodeCharacter{26A0}{\textbf{[!]}}       % ⚠
\DeclareUnicodeCharacter{2B50}{\textbf{[*]}}       % ⭐
\DeclareUnicodeCharacter{FE0F}{}                   % Variation Selector
% All remaining emoji - convert to generic markers
\newcommand{\emojimarker}[1]{\textbf{[#1]}}
\DeclareUnicodeCharacter{1F504}{\emojimarker{SYNC}}
\DeclareUnicodeCharacter{1F3E2}{\emojimarker{BUILD}}
\DeclareUnicodeCharacter{1F4A1}{\emojimarker{IDEA}}
\DeclareUnicodeCharacter{1F4C1}{\emojimarker{FOLDER}}
\DeclareUnicodeCharacter{1F4CA}{\emojimarker{CHART}}
\DeclareUnicodeCharacter{1F4DD}{\emojimarker{MEMO}}
\DeclareUnicodeCharacter{1F4E6}{\emojimarker{PKG}}
\DeclareUnicodeCharacter{1F50D}{\emojimarker{SEARCH}}
\DeclareUnicodeCharacter{1F527}{\emojimarker{TOOL}}
\DeclareUnicodeCharacter{1F389}{\emojimarker{PARTY}}
\DeclareUnicodeCharacter{1F3AF}{\emojimarker{TARGET}}
\DeclareUnicodeCharacter{1F680}{\emojimarker{ROCKET}}
\DeclareUnicodeCharacter{1F6A7}{\emojimarker{WORK}}
\DeclareUnicodeCharacter{1F6A8}{\emojimarker{ALERT}}
\DeclareUnicodeCharacter{1F6E0}{\emojimarker{WRENCH}}
''',
    'papersize': 'a4paper',
    'pointsize': '10pt',
    'figure_align': 'htbp',
    'inputenc': '',
    'utf8extra': '',
}

# -- Custom configuration ----------------------------------------------------
html_theme_options = {
    'canonical_url': '',
    'analytics_id': '',
    'logo_only': False,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'vcs_pageview_mode': '',
    'style_nav_header_background': '#2980B9',
    # Toc options
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}
