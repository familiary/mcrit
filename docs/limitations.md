# Known limitations

## One name per function body, where the linker folded several (#126)

Identical code folding (ICF) lets a linker keep a single copy of functions whose code is
byte-identical, so one address can carry several names that are all equally true - every no-op
`Drop` implementation of a Rust binary, for instance, can end up as one body. MCRIT stores one
function per address, as SMDA reports it, with the one name the report gives; taking function
names from a report as labels (`updateFunctionLabels`) likewise keeps one name per address. The
other names of the folded body are not recorded, unless a user adds them as labels by hand
(`function_labels` holds any number).

This bounds what attribution by name can reach. A match against such a body names it by the one
name stored for it, and which one says nothing about the others; scoring a match as wrong because
it named a different, equally true alias understates it. Scoring set-valued - a match is right if
it names any of the names folded onto that body - is the fair measure.

How much it matters depends on the target. On a self-built ripgrep 14.1.1, 295 of 6,924 function
addresses carried more than one name, up to 30 on one address, and scoring set-valued moved
full-name accuracy from 30.0% to 30.2%. It is recorded here rather than changed because the
fix starts in SMDA, which reports one name per function, and the gain measured so far is small.
`FunctionEntry.function_labels` is already a list, so no schema change is needed if SMDA ever
reports the aliases.
