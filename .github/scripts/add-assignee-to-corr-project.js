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
    // get issue info
    const issue = await octokit.rest.issues.get({
      owner: process.env.GITHUB_REPOSITORY_OWNER,
      repo: "matrixone",         
      issue_number: issueNumber,
    });
    // get assignees list
    const assignees = issue.data.assignees;
    // get issue node_id
    const issue_node_id = issue.data.node_id;
    if (assignees.length === 0) {
      console.log("The issue has not yet been assigned");
      return;
    }
    const projectMapping = {
      'compute-group-1': 33,
      'compute-group-2': 36,
      'storage-group': 35,
    };

    const projectsToAssociate = [];
    // get team_list in org
    const teams = await octokit.rest.teams.list({
        org: organizationLogin,  
      });
    // add team_slug to projectsToAssociate
    for(const team_data of teams.data){
      const team_member = await octokit.rest.teams.listMembersInOrg({
            org: organizationLogin,
            team_slug: team_data.slug,
          });
      for(const assignee of assignees){
        if(team_member.data.find((m) => m.login === assignee.login)){
          if(projectMapping[team_data.slug]){
            projectsToAssociate.push(projectMapping[team_data.slug]);
            console.log("success get one team_slug");
          }
        }
      }
    }

    if (projectsToAssociate.length === 0) {
      console.log("Put it in the default project");
      projectsToAssociate.push(13); 
    }
    // deduplicate
    const result = Array.from(new Set(projectsToAssociate))
    console.log(result)

    // graphql header
    const headers = {
        'Authorization': art,
        'Content-Type': 'application/json',
      };
    // get node-id by projectid ,push issue to project
    for (const projectId of result) {
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
      // get project node-id
      const resp = await fetch(githubApiEndpoint, options);
      const resp_json = await resp.json();
      pid = resp_json.data.organization.projectV2.id;
      console.log('Project ID:', pid);
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
      // push issue to project
      await fetch(githubApiEndpoint, options);
    }
  } catch (error) {
    console.log(error.message)
  }
}

run();
