# Copyright 2026 Matrix Origin
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

import ast
import re
import textwrap
from pathlib import Path

import pytest

DOC_ROOT = Path(__file__).parents[2]
ASYNC_SESSION_DOCS = [
    Path('README.md'),
    Path('README_USER.md'),
    Path('docs/quickstart.rst'),
]


def _parents(tree):
    return {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}


def _session_execute_calls(node):
    return [
        child
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr == 'execute'
        and isinstance(child.func.value, ast.Name)
        and child.func.value.id == 'session'
    ]


def _directly_awaited(call, parents):
    parent = parents.get(call)
    return isinstance(parent, ast.Await) and parent.value is call


def _async_usage_section(path, content):
    if path.suffix == '.rst':
        match = re.search(
            r'^Async Usage with Sessions and ORM\s*\n-+\s*\n([\s\S]*?)(?=^Vector Operations with Table Models\s*$|\Z)',
            content,
            re.MULTILINE,
        )
    else:
        match = re.search(r'^### Async Usage\s*$([\s\S]*?)(?=^### |\Z)', content, re.MULTILINE)
    assert match is not None, f'{path} should document async session usage'
    return match.group(1)


def _python_examples(path, section):
    if path.suffix == '.rst':
        lines = section.splitlines()
        blocks = []
        for index, line in enumerate(lines):
            if line.strip() != '.. code-block:: python':
                continue
            indented = []
            for candidate in lines[index + 1 :]:
                if not candidate.strip():
                    indented.append(candidate)
                    continue
                if len(candidate) - len(candidate.lstrip()) < 3:
                    break
                indented.append(candidate)
            if indented:
                blocks.append(textwrap.dedent('\n'.join(indented)))
        return blocks
    return re.findall(r'^```python\s*$([\s\S]*?)^```\s*$', section, re.MULTILINE)


@pytest.mark.parametrize('relative_path', ASYNC_SESSION_DOCS, ids=lambda path: path.as_posix())
def test_async_session_documentation_awaits_operations_sequentially(relative_path):
    section = _async_usage_section(relative_path, (DOC_ROOT / relative_path).read_text())
    code_blocks = _python_examples(relative_path, section)
    assert code_blocks, f'{relative_path} should include a Python example'

    session_blocks = []
    for code in code_blocks:
        tree = ast.parse(code)
        for context in ast.walk(tree):
            if not isinstance(context, ast.AsyncWith):
                continue
            if any(
                isinstance(item.optional_vars, ast.Name)
                and item.optional_vars.id == 'session'
                and isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Attribute)
                and item.context_expr.func.attr == 'session'
                for item in context.items
            ):
                session_blocks.append((tree, context))

    assert session_blocks, f'{relative_path} should demonstrate an async session'
    calls = [call for _, context in session_blocks for call in _session_execute_calls(context)]
    assert calls, f'{relative_path} should demonstrate session.execute()'
    for tree, context in session_blocks:
        parents = _parents(tree)
        for call in _session_execute_calls(context):
            assert _directly_awaited(call, parents), (
                f'{relative_path}: await each operation on one AsyncSession directly and sequentially; '
                'do not pass session.execute() calls to asyncio.gather().'
            )


def test_same_session_gather_is_rejected_by_the_documentation_contract():
    unsafe = ast.parse(
        'async def example(session):\n'
        '    await asyncio.gather(session.execute("SELECT 1"), session.execute("SELECT 2"))\n'
    )
    calls = _session_execute_calls(unsafe)
    parents = _parents(unsafe)

    assert len(calls) == 2
    assert not any(_directly_awaited(call, parents) for call in calls)
