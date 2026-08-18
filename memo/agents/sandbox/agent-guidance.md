Create dependency environments inside the recording root and ensure generated dependency
directories are ignored by Git. Keep source code, configuration, dependency manifests, and
lockfiles in the recording root. Use the conventional shared cache locations and ephemeral `/tmp`
for temporary files. Do not install system-wide or with `pip --user`. Ask the user when a missing
system dependency or large external dataset requires host setup or an additional sandbox grant.
Do not modify `.memo-sandbox`; ask the user to change sandbox permissions.
