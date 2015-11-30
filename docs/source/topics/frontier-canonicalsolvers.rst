.. _canonical-url-solver:

====================
Canonical URL Solver
====================

Is a special :ref:`middleware <frontier-middlewares>` object responsible for identifying canonical URL address of the
document and modifying request or response metadata accordingly. Canonical URL solver always executes last in the
middleware chain, before calling Backend methods.

The main purpose of this component is preventing metadata records duplication and confusing crawler behavior connected
with it. The causes of this are:
- Different redirect chains could lead to the same document.
- The same document can be accessible by more than one different URL.

Well designed system has it's own, stable algorithm of choosing the right URL for each document. Also see
`Canonical link element`_.

Canonical URL solver is instantiated during Frontera Manager initialization using class from :setting:`CANONICAL_SOLVER`
setting.

Built-in canonical URL solvers reference
========================================

Basic
-----
Used as default.

.. autoclass:: frontera.contrib.canonicalsolvers.basic.BasicCanonicalSolver


.. _Canonical link element: https://en.wikipedia.org/wiki/Canonical_link_element#Purpose