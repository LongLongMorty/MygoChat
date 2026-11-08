# 提交规范（Commit Convention）

本仓库所有提交（含 AI 生成/协助的提交）必须遵守以下规则。

## 一、AI 提交基础准则（所有 AI 提交必须遵守）

- **必须带 Co-Authored-By**：AI 生成或协助完成的每一条 commit，末尾都必须有一行 `Co-Authored-By` trailer，与正文之间空一行。`amend`、`revert` 等提交同样适用，不得遗漏。
- **填写当前真实使用的模型**：trailer 写本次会话实际运行的模型名称。无法确定当前模型时，使用默认兜底：
  ```
  Co-Authored-By: DeepSeek V4.1 Flash 1017 <noreply@deepseek.com>
  ```
- **默认使用简体中文**：commit 标题和正文默认用简体中文书写；类型前缀（如 `feat`、`fix`）、代码标识符、文件路径保持原文。
- 每条 commit **只写一行** AI 的 `Co-Authored-By`，不重复添加，不写与实际不符的模型名称。

## 二、Git 提交规范（每次提交必须遵守）

所有提交必须遵循 **Conventional Commits 1.0.0**，并在结尾附带 `Co-Authored-By` trailer（规则见上）。

### 格式

```
<type>[optional scope]: <描述>

[optional body]

Co-Authored-By: <当前模型名称> <noreply@anthropic.com>
```

### type 取值

| type | 含义 |
| --- | --- |
| `feat` | 新功能 |
| `fix` | 修复 bug |
| `docs` | 仅文档变更 |
| `style` | 不影响代码含义的变更（空格、格式化、分号等） |
| `refactor` | 既不修 bug 也不加功能的代码重构 |
| `perf` | 提升性能的代码变更 |
| `test` | 增加或修正测试 |
| `build` | 影响构建系统或外部依赖的变更（如 `go.mod`、`package.json`、打包脚本） |
| `ci` | CI 配置变更 |
| `chore` | 其他不修改 src 或测试的变更 |
| `revert` | 回滚某次提交 |

### 约定细则

- 描述用中文，祈使句，首字母/首字不加句号。
- `scope` 可选，用于标注影响范围（如 `feat(recording):`、`fix(ocr):`、`feat(tray):`）。
- 破坏性变更：在 `type`/`scope` 后加 `!`（如 `feat!:`），或在 body/footer 用 `BREAKING CHANGE:` 说明。
- body 与标题之间空一行；分点用 `-`。
- 每次提交结尾必须包含当前模型的 `Co-Authored-By`；识别不到模型时用兜底：
  ```
  Co-Authored-By: DeepSeek V4.1 Flash 1017 <noreply@deepseek.com>
  ```
- 优先原子提交：一次提交聚焦一个关注点/一个 `type`。

### 示例

```
fix(recording): 修正打包后 ffmpeg/OCR 资源路径缺 resources 前缀

- ffmpeg_exe_path/ocr_exe_path 生产分支 res.join(rel) 漏了 resources 段
- 导致打包分发后录屏/文字识别报“找不到”，回落到开发机写死路径

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```

## 三、PowerShell 提交注意事项

在 Windows PowerShell 终端执行 `git commit` 时，**不要用 here-string（`@'...'@`）传 `-m` 参数**——PowerShell 会把首行 `@'` 和末行 `'@` 的 `@` 也吞进提交信息，导致 subject 以 `@` 开头、正文以 `@` 结尾。

正确做法：把提交信息先写到临时文件，再用 `-F` 读入：

```powershell
git commit -F /path/to/commit-msg.txt
```

或仅用单行 `-m`（适合无 body 的简单提交）：

```powershell
git commit -m "fix: some short message"
```
