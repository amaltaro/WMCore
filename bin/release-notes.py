#!/usr/bin/env python
"""
Script used to build the WMCore release notes.
Originally taken from: https://github.com/cms-sw/cms-bot

Requires a third-party library: easy_install PyGithub

and a github token to access it via command line and/or APIs, see:
https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/
"""
import json
import re
import urllib2
from commands import getstatusoutput
from optparse import OptionParser
from os.path import exists, expanduser
from socket import setdefaulttimeout
from sys import exit

from github import Github

setdefaulttimeout(120)

REPO_URL = "https://api.github.com/repos/dmwm/WMCore"
CMSDIST_REPO_NAME = "dmwm/WMCore"
CMSSW_REPO_NAME = "dmwm/WMCore"


def prs2relnotes(notes, ref_repo=""):
    new_notes = {}
    for pr_num in notes:
        new_notes[pr_num] = format("- %(ref_repo)s#%(pull_request)s from @%(author)s: %(title)s",
                                   ref_repo=ref_repo,
                                   pull_request=pr_num,
                                   author=notes[pr_num]['author'],
                                   title=notes[pr_num]['title'])
        return new_notes


def get_merge_prs(prev_tag, this_tag, git_dir, repo, github, cache={}):
    print "Getting merged Pull Requests b/w", prev_tag, this_tag
    cmd = format("GIT_DIR=%(git_dir)s"
                 " git log --graph --merges --pretty='%%s: %%P' %(previous)s..%(release)s | "
                 " grep ' Merge pull request #[1-9][0-9]* from ' | "
                 " sed 's|^.* Merge pull request #||' | "
                 " sed 's|/[^:]*:||;s|from ||'",
                 git_dir=git_dir,
                 previous=prev_tag,
                 release=this_tag)
    error, notes = getstatusoutput(cmd)
    print "Getting Merged Commits:", cmd
    print notes
    if error:
        print "Error while getting release notes."
        print notes
        exit(1)
    return fill_notes_description(notes, repo, github, cache)


def api_rate_limits(gh, msg=True):
    gh.get_rate_limit()
    check_rate_limits(gh.rate_limiting[0], gh.rate_limiting[1], gh.rate_limiting_resettime, msg)


def check_rate_limits(rate_limit, rate_limit_max, rate_limiting_resettime, msg=True):
    from time import sleep, gmtime
    from calendar import timegm
    from datetime import datetime
    doSleep = 0
    rate_reset_sec = rate_limiting_resettime - timegm(gmtime()) + 5
    if msg: print 'API Rate Limit: %s/%s, Reset in %s sec i.e. at %s' % (
        rate_limit, rate_limit_max, rate_reset_sec, datetime.fromtimestamp(rate_limiting_resettime))
    if rate_limit < 100:
        doSleep = rate_reset_sec
    elif rate_limit < 500:
        doSleep = 30
    elif rate_limit < 1000:
        doSleep = 10
    elif rate_limit < 1500:
        doSleep = 5
    elif rate_limit < 2000:
        doSleep = 3
    elif rate_limit < 2500:
        doSleep = 1
    if (rate_reset_sec < doSleep): doSleep = rate_reset_sec
    if doSleep > 0:
        if msg: print "Slowing down for %s sec due to api rate limits %s approching zero" % (doSleep, rate_limit)
        sleep(doSleep)
    return


def fill_notes_description(notes, repo, github, cache={}):
    new_notes = {}
    for log_line in notes.splitlines():
        items = log_line.split(" ")
        author = items[1]
        pr_number = items[0]
        if cache and (pr_number in cache):
            new_notes[pr_number] = cache[pr_number]
            print 'Read from cache ', pr_number
            continue
        parent_hash = items.pop()
        pr_hash_id = pr_number + ":" + parent_hash
        if 'invalid_prs' in cache and pr_hash_id in cache['invalid_prs']: continue
        print "Checking ", pr_number, author, parent_hash
        try:
            api_rate_limits(github)
            pr = repo.get_pull(int(pr_number))
            ok = True
            if pr.head.user.login != author:
                print "  Author mismatch:", pr.head.user.login
                ok = False
            if pr.head.sha != parent_hash:
                print "  sha mismatch:", pr.head.sha
                ok = False
            if not ok:
                print "  Invalid/Indirect PR"
                cache_invalid_pr(pr_hash_id, cache)
                continue
            new_notes[pr_number] = {
                'author': author,
                'title': pr.title.encode("ascii", "ignore"),
                'user_ref': pr.head.ref.encode("ascii", "ignore"),
                'hash': parent_hash,
                'branch': pr.base.ref.encode("ascii", "ignore")}
            if not pr_number in cache:
                cache[pr_number] = new_notes[pr_number]
                cache['dirty'] = True
        except UnknownObjectException as e:
            print "ERR:", e
            cache_invalid_pr(pr_hash_id, cache)
            continue
    return new_notes


def format(s, **kwds):
    return s % kwds


# ---------------------------------------------------------
# pyGithub
# --------------------------------------------------------

#
# defines the categories for each pr in the release notes
#
def add_categories_notes(notes):
    for pr_number in notes:
        api_rate_limits(github)
        issue = CMSSW_REPO.get_issue(int(pr_number))
        categories = [l.name.split('-')[0] for l in issue.labels if
                      re.match("^[a-zA-Z0-9]+[-](approved|pending|hold|rejected)$", l.name)
                      and not re.match('^(tests|orp)-', l.name)]
        if len(categories) == 0:
            print "no categories for:", pr_number
        else:
            print "Labels for %s: %s" % (pr_number, categories)
        note = notes[pr_number]
        for cat in categories:
            note += " `%s` " % cat

        rel_notes = ""
        REGEX_RN = re.compile('^release(-| )note(s|)\s*:\s*', re.I)
        msg = issue.body.encode("ascii", "ignore").strip()
        if REGEX_RN.match(msg): rel_notes = rel_notes + REGEX_RN.sub('', msg).strip() + "\n\n"
        for comment in issue.get_comments():
            msg = comment.body.encode("ascii", "ignore").strip()
            if REGEX_RN.match(msg):
                # FIXME: Once status api is stable then reject the comment if -1 emoji is set
                rel_notes = rel_notes + REGEX_RN.sub('', msg).strip() + "\n\n"
        if rel_notes: note = note + "\n\n" + rel_notes
        notes[pr_number] = note
    return notes


def get_cmssw_notes(previous_release, this_release):
    if not exists("cmssw.git"):
        error, out = getstatusoutput("git clone --bare git@github.com:dmwm/WMCore.git")
        if error: parser.error("Error while checking out the repository:\n" + out)
    getstatusoutput("GIT_DIR=cmssw.git git fetch --all --tags")
    return prs2relnotes(get_merge_prs(previous_release, this_release, "WMCore.git", CMSSW_REPO, github))


#
# returns the comparison url to include in the notes
#
def get_comparison_url(previous_tag, current_tag, repo):
    return COMPARISON_URL % (repo, previous_tag, current_tag)


# --------------------------------------------------------------------------------
# Start of Execution
# --------------------------------------------------------------------------------

COMPARISON_URL = 'https://github.com/dmwm/%s/compare/%s...%s'

if __name__ == "__main__":
    parser = OptionParser(
        usage="%(progname) <previous-release> <this-release>")
    parser.add_option("-n", "--dry-run", help="Only print out release notes. Do not execute.",
                      dest="dryRun", default=False, action="store_true")
    opts, args = parser.parse_args()

    if len(args) != 2:
        parser.error("Wrong number or arguments")
    prev_release = args[0]
    curr_release = args[1]

    # ---------------------------------
    # pyGithub intialization
    # ---------------------------------

    token = open("/data/github-token").read().strip()
    github = Github(login_or_token=token)
    CMSSW_REPO = github.get_repo(CMSSW_REPO_NAME)

    cmssw_notes = get_cmssw_notes(prev_release, curr_release)

    cmssw_notes = add_categories_notes(cmssw_notes)
    cmssw_notes_str = ""
    for pr in sorted(cmssw_notes.keys(), reverse=True):
        cmssw_notes_str += cmssw_notes[pr] + '\n'

    request = urllib2.Request(REPO_URL + "/releases?per_page=100",
                              headers={"Authorization": "token " + token})
    releases = json.loads(urllib2.urlopen(request).read())
    matchingRelease = [x["id"] for x in releases if x["name"] == curr_release]
    if len(matchingRelease) < 1:
        print "Release %s not found." % curr_release
        exit(1)

    releaseId = matchingRelease[0]
    url = REPO_URL + "/releases/%s" % releaseId
    request = urllib2.Request(url, headers={"Authorization": "token " + token})
    request.get_method = lambda: 'PATCH'
    print "Modifying release notes for %s at %s" % (curr_release, url)
    if opts.dryRun:
        print cmssw_notes_str
        print "--dry-run specified, quitting without modifying release."
        print 'ALL_OK'
        exit(0)

    header = "#### Changes since %s:\n%s\n" % \
             (prev_release, get_comparison_url(prev_release, curr_release, 'WMCore'))

    #    print urllib2.urlopen(request,
    #                          json.dumps({"body": header + cmssw_notes_str + cmsdist_header + cmsdist_notes_str})).read()
    print 'ALL_OK'
