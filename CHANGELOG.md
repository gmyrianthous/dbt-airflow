## 2.5.0 (2023-11-03)

### Feat

- **core**: Add support for '--exclude' flag (#108)

## 2.4.0 (2023-11-03)

### Feat

- **core**: Add support for multiple selectors (#106)

## 2.3.0 (2023-11-02)

### Feat

- **core**: Add support for (#103)

## 2.2.0 (2023-11-02)

### Feat

- **operators**: Add support for full-refresh (#102)

## 2.1.0 (2023-11-01)

### Feat

- **core**: Create enum class to represent available execution operators (#101)

## 2.0.0 (2023-10-31)

### Feat

- **config**: Create config classes (#98)

## 1.1.3 (2023-10-31)

### Refactor

- **opreators**: Refactor operator classes and introduce multiple inheritence (#95)

## 1.1.2 (2023-10-27)

### Fix

- **parser**: Allow empty nodes in NodeDeps model (#94)

## 1.1.1 (2023-10-12)

### Fix

- **core**: Remove unused modules (#92)

## 1.1.0 (2023-10-12)

### Feat

- **taskgroup**: Add BashOperator as the default operator_class (#90)

## 1.0.0 (2023-10-11)

## 0.3.2 (2023-05-12)

### Fix

- **parser**: Add operation in DbtResourceType Enum (#73)

## 0.3.1 (2023-04-28)

### Fix

- **cicd**: Fix trigger for docs deployment workflow (#66)

## 0.3.0 (2023-04-28)

### Feat

- **docs/cicd**: Automate the deployment of docs (#64)

## 0.2.0 (2023-02-24)

### Feat

- **repo**: Add CODEOWNERS (#61)

## 0.1.0 (2023-02-24)

### Feat

- **ci**: Enable automated releases (#60)
- **ci**: Add commitizen in pyproject (#32)
- **ci**: Enable semantic versioning
- **core**: Implement functionality for adding extra tasks in dbt TaskGroup (#25)
- **docker**: Update Dockerfile to create end-to-end dev environment (#21)
- **gh_actions**: Publish package on PyPI via poetry (#12)
- **core**: Implement core logic  (#4)
- **github_workflows**: Add workflow for automating package build and release on PyPI (#7)
- **gh**: Add workflow for tests (#3)
- **dbt_airflow**: Create initial structure (#2)

### Fix

- **ci**: Disable credentials persistance in checkout action (#59)
- **ci**: Update the way we make releases (#58)
- **ci**: Add pre-commit hook (#57)
- **ci**: Update trigger (#56)
- **ci**: Fix personal access token in release step (#55)
- **ci**: Update gh token (#54)
- **ci**: Update github token (#53)
- **ci**: Cleanup CI (#52)
- **ci**: Fix CI (#51)
- **ci**: Fix PR creation (#49)
- **ci**: Create PR on release (#48)
- **ci**: Add github action bot as committer (#47)
- **ci**: Add caching and remove changelog (#46)
- **ci**: Add cz command in Ci (#45)
- **ci**: Force push in CI (#44)
- **ci**: Add github token in last step of workflow (#42)
- **ci**: Fix date variable in main workflow (#41)
- **ci**: Fix main workflow (#40)
- **ci**: Fix git configuration (#39)
- **ci**: Fix git configuration in main workflow (#38)
- **ci**: Fix git configuration in CI (#36)
- **ci**: Update git configuration (#35)
- **ci**: Fix git configuration in main workflow (#34)
- **ci**: Fix trigger on deploy workflow (#31)
- **gh_actions**: Remove condition from publish job (#14)
