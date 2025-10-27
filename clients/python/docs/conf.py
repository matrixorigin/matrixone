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
% Complete emoji and special character support for PDF generation
\DeclareUnicodeCharacter{221A}{\ensuremath{\surd}}
\DeclareUnicodeCharacter{2588}{\rule{0.6em}{1em}}
\DeclareUnicodeCharacter{FE0F}{}
\newcommand{\emojimarker}[1]{\textbf{[#1]}}
\DeclareUnicodeCharacter{26A0}{\emojimarker{WARN}}
\DeclareUnicodeCharacter{26A1}{\emojimarker{BOLT}}
\DeclareUnicodeCharacter{2705}{\emojimarker{YES}}
\DeclareUnicodeCharacter{2713}{\emojimarker{CHK}}
\DeclareUnicodeCharacter{2717}{\emojimarker{X}}
\DeclareUnicodeCharacter{2728}{\emojimarker{STAR}}
\DeclareUnicodeCharacter{274C}{\emojimarker{NO}}
\DeclareUnicodeCharacter{1F310}{\emojimarker{GLOB}}
\DeclareUnicodeCharacter{1F381}{\emojimarker{GIFT}}
\DeclareUnicodeCharacter{1F389}{\emojimarker{PARTY}}
\DeclareUnicodeCharacter{1F3A8}{\emojimarker{ART}}
\DeclareUnicodeCharacter{1F3AF}{\emojimarker{TGT}}
\DeclareUnicodeCharacter{1F3E2}{\emojimarker{BUILD}}
\DeclareUnicodeCharacter{1F3EA}{\emojimarker{SHOP}}
\DeclareUnicodeCharacter{1F41B}{\emojimarker{BUG}}
\DeclareUnicodeCharacter{1F44B}{\emojimarker{WAVE}}
\DeclareUnicodeCharacter{1F464}{\emojimarker{USER}}
\DeclareUnicodeCharacter{1F465}{\emojimarker{USERS}}
\DeclareUnicodeCharacter{1F4A1}{\emojimarker{IDEA}}
\DeclareUnicodeCharacter{1F4AC}{\emojimarker{CHAT}}
\DeclareUnicodeCharacter{1F4B0}{\emojimarker{MONEY}}
\DeclareUnicodeCharacter{1F4BB}{\emojimarker{PC}}
\DeclareUnicodeCharacter{1F4BC}{\emojimarker{CASE}}
\DeclareUnicodeCharacter{1F4BE}{\emojimarker{DISK}}
\DeclareUnicodeCharacter{1F4C2}{\emojimarker{DIR}}
\DeclareUnicodeCharacter{1F4C8}{\emojimarker{UP}}
\DeclareUnicodeCharacter{1F4CA}{\emojimarker{CHART}}
\DeclareUnicodeCharacter{1F4CB}{\emojimarker{CLIP}}
\DeclareUnicodeCharacter{1F4D1}{\emojimarker{BOOK}}
\DeclareUnicodeCharacter{1F4D6}{\emojimarker{READ}}
\DeclareUnicodeCharacter{1F4DA}{\emojimarker{BOOKS}}
\DeclareUnicodeCharacter{1F4DC}{\emojimarker{SCROLL}}
\DeclareUnicodeCharacter{1F4DD}{\emojimarker{MEMO}}
\DeclareUnicodeCharacter{1F4E4}{\emojimarker{OUT}}
\DeclareUnicodeCharacter{1F4E5}{\emojimarker{IN}}
\DeclareUnicodeCharacter{1F4E6}{\emojimarker{PKG}}
\DeclareUnicodeCharacter{1F4E7}{\emojimarker{MAIL}}
\DeclareUnicodeCharacter{1F4F8}{\emojimarker{CAM}}
\DeclareUnicodeCharacter{1F504}{\emojimarker{SYNC}}
\DeclareUnicodeCharacter{1F50D}{\emojimarker{MAG}}
\DeclareUnicodeCharacter{1F50E}{\emojimarker{ZOOM}}
\DeclareUnicodeCharacter{1F517}{\emojimarker{LINK}}
\DeclareUnicodeCharacter{1F525}{\emojimarker{FIRE}}
\DeclareUnicodeCharacter{1F527}{\emojimarker{TOOL}}
\DeclareUnicodeCharacter{1F5C4}{\emojimarker{CAB}}
\DeclareUnicodeCharacter{1F680}{\emojimarker{ROCK}}
\DeclareUnicodeCharacter{1F6A7}{\emojimarker{WORK}}
\DeclareUnicodeCharacter{1F6A8}{\emojimarker{ALERT}}
\DeclareUnicodeCharacter{1F6D2}{\emojimarker{CART}}
\DeclareUnicodeCharacter{1F6E0}{\emojimarker{WRCH}}
\DeclareUnicodeCharacter{1F6E1}{\emojimarker{SHIELD}}
\DeclareUnicodeCharacter{1F91D}{\emojimarker{SHAKE}}
\DeclareUnicodeCharacter{1F9E0}{\emojimarker{BRAIN}}
\DeclareUnicodeCharacter{1F9EA}{\emojimarker{TEST}}
\DeclareUnicodeCharacter{1F9F9}{\emojimarker{CLEAN}}
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
