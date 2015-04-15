from docutils.parsers.rst.roles import set_classes
from docutils import nodes


REPO = 'https://github.com/scrapinghub/frontera/'


def setup(app):
    app.add_crossref_type(
        directivename="setting",
        rolename="setting",
        indextemplate="pair: %s; setting",
    )
    app.add_role('source', source_role)
    app.add_role('commit', commit_role)
    app.add_role('issue', issue_role)
    app.add_role('rev', rev_role)


def source_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    ref = REPO + 'blob/master/' + text
    set_classes(options)
    node = nodes.reference(rawtext, text, refuri=ref, **options)
    return [node], []


def issue_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    ref = REPO + 'issues/' + text
    set_classes(options)
    node = nodes.reference(rawtext, 'issue ' + text, refuri=ref, **options)
    return [node], []


def commit_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    ref = REPO + 'commit/' + text
    set_classes(options)
    node = nodes.reference(rawtext, text, refuri=ref, **options)
    return [node], []

def rev_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    ref = REPO + 'changeset/' + text
    set_classes(options)
    node = nodes.reference(rawtext, 'r' + text, refuri=ref, **options)
    return [node], []