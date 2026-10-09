const fs = require('node:fs');

const MARKER = '<!-- api-compatibility-report -->';
const SHA = /^[a-f0-9]{40}$/;

function readReport(path) {
  // Artifacts come from an untrusted PR. Read bounded JSON, never code or HTML.
  const stat = fs.lstatSync(path);
  if (!stat.isFile()) throw new Error('API report must be a regular file');
  if (stat.size > 300000) throw new Error('API report is too large');
  const report = JSON.parse(fs.readFileSync(path, 'utf8'));
  if (!Number.isSafeInteger(report.number) || report.number < 1 ||
      !SHA.test(report.head) || !SHA.test(report.base) ||
      !(report.exit_code === null || Number.isInteger(report.exit_code)) ||
      typeof report.output !== 'string') {
    throw new Error('Invalid API compatibility report');
  }
  return report;
}

function renderReport(report, run, serverUrl, repository) {
  const result = report.exit_code === 0
    ? '✅ No API breaking changes detected.'
    : report.exit_code === 100
      ? '⚠️ API breaking changes detected.'
      : 'ℹ️ API compatibility check could not complete.';
  // Escape compiler output and crate documentation so they cannot inject
  // Markdown, HTML, or notification mentions into a privileged bot comment.
  const text = report.output.length > 10000
    ? '[Output truncated; see workflow logs for the full report.]\n' + report.output.slice(-10000)
    : report.output;
  const output = text
    .replace(/\u001b\[[0-9;]*m/g, '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/@/g, '@\u200b');
  const runUrl = `${serverUrl}/${repository}/actions/runs/${run.id}/attempts/${run.run_attempt}`;
  return `${MARKER}
<!-- api-compatibility-run:${run.id}:${run.run_attempt} -->
### API compatibility

${result}

Checked PR commit \`${report.head}\` merged with base commit \`${report.base}\` for the workspace crates, with all features enabled.

This report is advisory and does not block merging. The checker does not detect every possible breaking change.

<details>
<summary>Checker output</summary>

<pre>${output}</pre>
</details>

[Full workflow logs](${runUrl})
`;
}

async function postReport({ github, context, core, reportPath }) {
  const run = context.payload.workflow_run;
  // Do not accept reports from another workflow with the same display name.
  if (run.event !== 'pull_request' || run.path !== '.github/workflows/api-compatibility.yml' ||
      run.conclusion === 'cancelled') return;
  const report = readReport(reportPath);
  if (report.head !== run.head_sha) throw new Error('Report does not match the triggering run');

  const { owner, repo } = context.repo;
  const repository = `${owner}/${repo}`;
  // workflow_run.pull_requests can be empty, including for fork PRs. Resolve
  // the supplied number through GitHub and verify its repository and commit.
  const { data: pr } = await github.rest.pulls.get({ owner, repo, pull_number: report.number });
  if (pr.state !== 'open' || pr.base.repo.full_name !== repository ||
      pr.base.ref !== context.payload.repository.default_branch || pr.head.sha !== report.head) {
    core.info('Skipping a closed or outdated PR report.');
    return;
  }

  const comments = await github.paginate(github.rest.issues.listComments, {
    owner, repo, issue_number: report.number, per_page: 100,
  });
  const existing = comments.find(comment =>
    comment.user?.login === 'github-actions[bot]' && comment.body?.startsWith(MARKER));
  const previous = existing?.body.match(/<!-- api-compatibility-run:(\d+):(\d+) -->/);
  if (previous && (Number(previous[1]) > run.id ||
      (Number(previous[1]) === run.id && Number(previous[2]) > run.run_attempt))) {
    core.info('A newer run has already reported on this PR.');
    return;
  }

  const body = renderReport(report, run, context.serverUrl, repository);
  if (existing) {
    await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id, body });
  } else {
    await github.rest.issues.createComment({ owner, repo, issue_number: report.number, body });
  }
}

module.exports = { readReport, renderReport, postReport };
