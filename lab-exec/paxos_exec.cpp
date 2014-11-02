// Basic routines for Paxos implementation

#include "make_unique.h"
#include "paxmsg.h"
#include "paxserver.h"
#include "log.h"
#include <string>

using namespace std;

void paxserver::execute_arg(const struct execute_arg& ex_arg)
{
    // if this is a duplicate request, do nothing
    if (paxlog.find_rid(ex_arg.nid, ex_arg.rid))
    {
        //LOG(l::DEBUG, "execute_arg msg for nid " << ex_arg.nid
        //    << "and rid " << ex_arg.rid
        //    << "received earlier (duplicate)\n");
        //LOG(l::DEBUG, "exec_arg duplicate: " << ex_arg << "\n");
        return;
    }
    // create the viwestamp to send backups
    ts++;
    viewstamp_t new_vs;
    new_vs.vid = vc_state.view.vid;
    new_vs.ts = ts;
    // primary first logs this request
    paxlog.log(ex_arg.nid, ex_arg.rid, new_vs, ex_arg.request, get_serv_cnt(vc_state.view), net->now());
    // send the replicate_arg to all the backup cohorts
    if (primary())
    {
        std::set<node_id_t> servers = get_other_servers(vc_state.view);
        for(const auto& serv : servers)
        {
           // LOG(l::DEBUG, id_str() << "pr replicate_arg msg now: " << net->now()
           //    << "to server nid: " << serv << "\n");
            send_msg(serv, std::make_unique<replicate_arg>(new_vs, ex_arg, latest_seen));
            
        }
    }
    else // otherwise send a not ok message to client with the id of the primary
    {
        //LOG(l::DEBUG, id_str() << "is not a primary now: " << net->now
        //    << " send not ok to client rid: " << ex_arg.nid << "\n");
        send_msg(ex_arg.nid, std::make_unique<execute_fail>(vc_state.view.vid, vc_state.view.primary, ex_arg.rid));
    }
    return;
   //MASSERT(0, "execute_arg not implemented\n");
}

void paxserver::replicate_arg(const struct replicate_arg& repl_arg) {
    // log the current request if not already logged
    if (! paxlog.get_tup(repl_arg.arg.vs)) {
        //LOG(l::DEBUG, id_str() << " logging repl_arg msg from primary now: " << net->now
        //   << "  " << repl_arg << "\n");
        paxlog.log(repl_arg.arg.nid, repl_arg.arg.rid, repl_arg.vs, repl_arg.arg.request, get_serv_cnt(vc_state.view), net->now());
    }
    // execute all the requests <= the committed recvd from primary (DOUBT : should it be just less than (as in paper?))
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
        
        if (paxlog.next_to_exec(it) && (*it)->vs <= repl_arg.committed) {
            //LOG(l::DEBUG, id_str() << " executing repl_arg msg from primary now\n");
            // DOUBT : does the next_to_exec automatically update the latest_exec?
            paxop_on_paxobj(*it);
            paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
        }
        
    }
    // trim the log as possible - remove all the executed entries (they should be <= committed)
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>&)->bool{return tup->executed;});
    // send repl_res ack to primary
    // LOG(l::DEBUG, id_str() << " sending repl_res msg to primary now "
    //    << " for vs : "<< repl_arg.vs << "\n");
    send_msg(vc_state.view.primary, std::make_unique<replicate_res>(repl_arg.vs));
    return;
   //MASSERT(0, "replicate_arg not implemented\n");
}

void paxserver::replicate_res(const struct replicate_res& repl_res) {
   // if paxlog is empty or has no unexecuted entry, send accept_arg to cohorts
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>&)->bool{return (tup->executed && (tup->resp_cnt == tup->serv_cnt);});
    if (paxlog.empty()) {
        std::set<node_id_t> servers = get_other_servers(vc_state.view);
        for(const auto& serv : servers)
        {
            //LOG(l::DEBUG, id_str() << "pr accept_arg msg now: " << net->now()
            //    << "to server nid: " << serv << "\n");
            send_msg(serv, std::make_unique<accept_arg>(latest_seen));
            
        }
        return;
    }
    paxlog.incr_resp(repl_res.vs);
   // otherwise execute all the requests possible on the primary and send corresponding success messages to client
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
        
        if (paxlog.next_to_exec(it) && ((*it)->resp_cnt >= ((*it)->serv_cnt/2 + 1))) {
            //LOG(l::DEBUG, id_str() << " executing repl_res msg on primary.\n");
            // DOUBT : does the next_to_exec automatically update the latest_exec?
            string result = paxop_on_paxobj(*it);
            paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
            // send success message to client
            //LOG(l::DEBUG, id_str() << " sending success msg from primary now : " << net->now()
            //    << " result: "<< result << "rid : " << (*it)->rid << "\n");
            send_msg((*it)->src, std::make_unique<execute_success>(result, (*it)->rid));
             // update the commited field to the latest executed viewstamp
            vc_state.latest_seen = (*it)->vs
            
        }
        
    }
    // try trimming the log again
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>&)->bool{return (tup->executed && (tup->resp_cnt == tup->serv_cnt));});
    return;
   //MASSERT(0, "replicate_res not implemented\n");
}

void paxserver::accept_arg(const struct accept_arg& acc_arg) {
    // execute all the requests <= the committed recvd from primary
    for(auto it = paxlog.begin(); it != paxlog.end(); ++it)
    {
        if (paxlog.next_to_exec(it) && (*it)->vs <= acc_arg.committed) {
            //LOG(l::DEBUG, id_str() << " executing acc_arg msg from primary \n");
            // DOUBT : does the next_to_exec automatically update the latest_exec?
            paxop_on_paxobj(*it);
            paxlog.execute(*it); //DOUBT maybe this is handled by paxop_on_paxobj
        }
        
    }
    // trim the log as possible - remove all the executed entries (they should be <= committed)
    paxlog.trim_front([](const std::unique_ptr<Paxlog::tup>&)->bool{return tup->executed;});
    return;
   //MASSERT(0, "accept_arg not implemented\n");
}
