// Basic routines for Paxos implementation

#include "make_unique.h"
#include "paxmsg.h"
#include "paxserver.h"
#include "log.h"
#include <string>
#include <cstdio>

void paxserver::execute_arg(const struct execute_arg& ex_arg)
{   
    // send the replicate_arg to all the backup cohorts
    if (primary())
    {
         // if this is a duplicate request, do nothing
	 if (paxlog.find_rid(ex_arg.nid, ex_arg.rid))
	 {
	     return;
	 }
	// create the viwestamp to send backups
	viewstamp_t new_vs;
	new_vs.vid = vc_state.view.vid;
	new_vs.ts = ts;
	ts++;
	// primary first logs this request
	paxlog.log(ex_arg.nid, ex_arg.rid, new_vs, ex_arg.request, get_serv_cnt(vc_state.view), net->now());
        std::set<node_id_t> servers = get_other_servers(vc_state.view);
        for(const auto& serv : servers)
        {
            send_msg(serv, std::make_unique<struct replicate_arg>(new_vs, ex_arg, vc_state.latest_seen));
        }
    }
    else 
    {
      // otherwise send a not ok message to client with the id of the primary  
      send_msg(ex_arg.nid, std::make_unique<struct execute_fail>(vc_state.view.vid, vc_state.view.primary, ex_arg.rid));
    }
    return;
}

void paxserver::replicate_arg(const struct replicate_arg& repl_arg) {
  if ((paxlog.get_tup(repl_arg.vs) == nullptr)) 
    {
        paxlog.log(repl_arg.arg.nid, repl_arg.arg.rid, repl_arg.vs, repl_arg.arg.request, get_serv_cnt(vc_state.view), net->now());
    }
    // execute all the requests <= the committed recvd from primary (DOUBT : should it be just less than (as in paper?))
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
        if (paxlog.next_to_exec(it) && (*it)->vs <= repl_arg.committed) 
        {
            paxop_on_paxobj(*it);
	    paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
        }
        
    }
    // trim the log as possible - remove all the executed entries (they should be <= committed)
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>& tup)->bool{return tup->executed;});
    // send repl_res ack to primary
    send_msg(vc_state.view.primary, std::make_unique<struct replicate_res>(repl_arg.vs));
    return;
}

void paxserver::replicate_res(const struct replicate_res& repl_res) 
{
   // if paxlog is empty or has no unexecuted entry, send accept_arg to cohorts
  bool all_executed = true;
  for(auto it = paxlog.begin(); it != paxlog.end(); ++it) {
    if(!(*it)->executed) {
      all_executed = false;
      break;
    }
  }
  if (paxlog.empty() || all_executed) {
        std::set<node_id_t> servers = get_other_servers(vc_state.view);
        for(const auto& serv : servers)
        {
            send_msg(serv, std::make_unique<struct accept_arg>(vc_state.latest_seen));
            
        }
        return;
    }
   paxlog.incr_resp(repl_res.vs);
   // otherwise execute all the requests possible on the primary and send corresponding success messages to client
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
	if ((*it)->resp_cnt > (*it)->serv_cnt/2 && paxlog.next_to_exec(it)) 
	{	
            // DOUBT : does the next_to_exec automatically update the latest_exec?
            std::string result = paxop_on_paxobj(*it);
            paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
            // send success message to client
            send_msg((*it)->src, std::make_unique<struct execute_success>(result, (*it)->rid));
            // update the commited field to the latest executed viewstamp
            vc_state.latest_seen = (*it)->vs;
            
        }        
    }
    // try trimming the log again
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>& tup)->bool{return (tup->executed && (tup->resp_cnt == tup->serv_cnt));});
    return;
}

void paxserver::accept_arg(const struct accept_arg& acc_arg) {
    // execute all the requests <= the committed recvd from primary
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
        if (paxlog.next_to_exec(it) && (*it)->vs <= acc_arg.committed) {
            // DOUBT : does the next_to_exec automatically update the latest_exec?
            paxop_on_paxobj(*it);
            paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
        }
        
    }
    // trim the log as possible - remove all the executed entries (they should be <= committed)
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>& tup)->bool{return tup->executed;});
    return;
}
