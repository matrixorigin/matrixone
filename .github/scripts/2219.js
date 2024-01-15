const { Octokit } = require("@octokit/action");
const fetch = require("node-fetch");

const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});
const githubApiEndpoint = "https://api.github.com/graphql";

const organizationLogin = "matrixorigin";
const token = process.env.GITHUB_TOKEN;
const art = "Bearer "+token;
async function run() {
  try {
    const issueNumber = process.env.Issue_ID;
    // 获取 issue 的详细信息
    const issue = await octokit.rest.issues.get({
      owner: process.env.GITHUB_REPOSITORY_OWNER,
      repo: "matrixone",         
      issue_number: issueNumber,
    });
    // 获取assignees列表
    const assignees = issue.data.assignees;
    // 获取issue的node_id
    const issue_node_id = issue.data.node_id;
    if (assignees.length === 0) {
      console.log("Issue 没有 assignee，不进行项目关联");
      return;
    }
    const projectMapping = {
      'compute-group-1': 33,
      'compute-group-2': 36,
      'storage-group': 35,
    };

    const projectsToAssociate = [];
    // 获取org下的team列表
    const teams = await octokit.rest.teams.list({
        org: organizationLogin,  
      });
    //判断team下是否包含某个成员,是就将映射的projectid放到projectsToAssociate里
    for(const team_data of teams.data){
      const team_member = await octokit.rest.teams.listMembersInOrg({
            org: organizationLogin,
            team_slug: team_data.slug,
          });
      // console.log("team_member",team_member);
      for(const assignee of assignees){
        if(team_member.data.find((m) => m.login === assignee.login)){
          if(projectMapping[team_data.slug]){
            projectsToAssociate.push(projectMapping[team_data.slug]);
            console.log("成功push一个信息");
          }
        }
      }
    }

    if (projectsToAssociate.length === 0) {
      console.log("没有team，放到默认project下");
      projectsToAssociate.push(13); 
    }
    // 去重，获取的projectid可能有重复，因为一个assignee可以在多个的team下，
    const result = Array.from(new Set(projectsToAssociate))
    console.log(result)

    // graphql  的header
    const headers = {
        'Authorization': art,
        'Content-Type': 'application/json',
      };
    //根据projectid获取对应的node-id，然后插入issue
    for (const projectId of result) {
    // 通过graphql获取node-id的query
    var query = `
        query {
          organization(login: "${organizationLogin}") {
            projectV2(number: ${projectId}) {
              id
            }
          }
        }
      `;
      var options = {
          method: 'POST',
          headers: headers,
          body: JSON.stringify({ query }),
        };
      let pid;   // 存node-id
      // 获取node-id的请求
      const resp = await fetch(githubApiEndpoint, options);
      const resp_json = await resp.json();
      pid = resp_json.data.organization.projectV2.id;
      console.log('Project ID:', pid);
      // 通过graphql向project插入issue的query
      var query=`
          mutation{
            addProjectV2ItemById(input:{projectId: \"${pid}\" contentId: \"${issue_node_id}\" }){
                item  {
                   id   
                  }
                }
          }
        `;
      var options = {
          method: 'POST',
          headers: headers,
          body: JSON.stringify({ query }),
        };
      // 向project中插入issue
      const resp_add = await fetch(githubApiEndpoint, options);
        const resp_add_json = await resp_add.json();
    }
  } catch (error) {
    console.log(error.message)
  }
}

run();
