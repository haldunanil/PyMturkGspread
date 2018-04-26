import pandas as pd
import numpy as np
import boto
from boto.mturk.connection import *
from oauth2client.service_account import ServiceAccountCredentials
from scipy import stats
import matplotlib as mpl

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools


class Survey(object):
    """
    The Survey class for the mturk project.

    This class takes into account any activity regarding a Survey conducted
    through the Amazon Mturk portal. This class presumes that the Survey on
    Mturk is primarily used as:
        - a way to obtain respondents
        - a way to screen respondents

    If the user wishes to conduct the Survey entirely on Mturk without a
    screener, simply set up the Survey, run self.get_mturk_results, and use the
    mturk_results pandas file.

    If the Survey is set up such that Mturk only contains the screener
    question(s), then follow the following steps:
        - call 'self.add_conditions' for each desired filtering parameter
        - call 'self.filter_mturk_results' to get the screened out group
        - call 'self.send_first_mailer' to inform users about the second part
          of the Survey
        - call 'self.send_second_mailer' to remind them, if desired
        - call 'self.award_bonus' if want to add bonus

    Note that in order to use send_second_mailer, you MUST use a subclass that
    is linked up to the results of the second part of the Survey. Currently
    supported platforms are:
        - Google Forms
        - SurveyMonkey (under construction)

    To call self.send_second_mailer, please use one of the appropriate
    subclasses.
    """

    def __init__(self, access_key, secret_access_key, HITlist,
                 questions, srvy_link, from_name):
        """
        Initialize the Survey class.

        :param access_key: mturk access key; found on the developer portal
        :param secret_access_key: mturk secret access key; found on the developer
                              portal
        :param HITlist: a list containing desired HITId strings in a list
        :param questions: a list containing the names of the screener questions as
                      they appear on Mturk
        :param srvy_link: a url string leading to part 2 of the Survey
        """
        self.mturk = boto.mturk.connection.MTurkConnection(access_key,
                                                           secret_access_key)
        self.HITlist = HITlist
        self.questions = questions
        self.srvy_link = srvy_link
        self.from_name = from_name

        try:
            bal = self.mturk.get_account_balance()[0]
            print('\nConnection Successful! Current balance is:', bal, '\n')
        except:
            raise ValueError('Connection error!')

    def add_conditions(self, *conditions):
        """
        Add filtering conditions for the screener questions.

        :param conditions: List of conditions expressed in string form. Should be
                           expressed as follows: 'Login != None'

                           Permitted operands are:
                           ==, !=, >=, <=, contains, does not contain
        """
        self.cond_mapping = {
            '==': lambda x, y, z: x[x[y] == z],
            '!=': lambda x, y, z: x[x[y] != z],
            '>=': lambda x, y, z: x[x[y] >= z],
            '<=': lambda x, y, z: x[x[y] <= z],
            # 'isin': lambda x, y, z: x[x[y].isin(z)],
            # 'not isin': lambda x, y, z: x[~x[y].isin(z)],
            'contains': lambda x, y, z: x[x[y].str.contains(z)],
            'does not contain': lambda x, y, z: x[~x[y].str.contains(z)]
        }

        for cond in conditions:
            if not any(f in cond for f in self.cond_mapping):
                raise ValueError('Operand in ' + cond + ' not permitted.')

        self.conditions = conditions


    def get_mturk_results(self, hit_id, questionList):
        """
        Return the full results of a HIT.

        :param hit_id: MTurk's HIT ID for the task
        :param questionList: List of questions asked
        :return: pandas array with HIT ID, Worker ID, Assignment ID,
                 and responses to all the questions
        """
        def helper(hit_id, question):
            """Helper function, runs for each response and returns a panda."""
            result, assignments = [], []

            i = 1
            while True:
                assignments_subset = self.mturk.get_assignments(hit_id,
                                                                page_size=100,
                                                                page_number=i)

                if len(assignments_subset) > 0:
                    assignments += assignments_subset
                    i += 1
                else:
                    break

            for assignment in assignments:
                question_form_answers = assignment.answers[0]
                for question_form_answer in question_form_answers:
                    if question_form_answer.qid == question:
                        user_response = question_form_answer.fields[0]
                        result.append([hit_id,
                                       assignment.WorkerId,
                                       assignment.AssignmentId,
                                       user_response])

            panda = pd.DataFrame(result, columns=['HITID', 'WorkerID',
                                                  'AssignmentID', question])

            return panda

        allPandas = []
        for question in questionList:
            df = helper(hit_id, question)
            allPandas.append(df)

        try:
            mergedPanda = allPandas[0]
            for panda in allPandas[1:]:
                mergedPanda = pd.merge(mergedPanda, panda,
                                       on=['HITID', 'WorkerID',
                                           'AssignmentID'])
        except ValueError:
            print('There are no values!')
            return None

        self.mturk_resp = mergedPanda
        return self.mturk_resp

    def filter_mturk_results(self):
        """
        Take in strings with conditions and create a Pandas DataFrame.

        Df should have results according to screening conditions. See
        self.add_conditions for permitted condition syntax.
        """
        def recurse(List):
            """Recursively find the intersection of all conditions."""
            if len(List) == 1:
                return List[0]

            x, y = List[0], List[1]
            z = x[x.index.isin(y.index)]
            newList = [z] + List[2:]

            return recurse(newList)

        valid_results = pd.concat(
            [
                self.get_mturk_results(hit_id, self.questions)
                for hit_id in self.HITlist
            ]
        )

        if len(self.conditions) > 0:
            results = []
            for cond in self.conditions:
                op = [x for x in self.cond_mapping if x in cond][0]
                var = cond.split(' ' + op + ' ')

                res = self.cond_mapping[op](valid_results, var[0], var[1])
                results.append(res)

            self.filtered_mturk_resp = recurse(results)

        else:
            self.filtered_mturk_resp = valid_results.copy()

        self.allAssignments = [str(i) for i in list(valid_results['AssignmentID'])]
        self.allUsers = [str(i) for i in list(valid_results['WorkerID'])]

        self.filteredAssignments = [
            str(i) for i in list(self.filtered_mturk_resp['AssignmentID'])]
        self.filteredUsers = [
            str(i) for i in list(self.filtered_mturk_resp['WorkerID'])]

    def return_all_users(self):
        """Return all the users that have completed the screener questions."""
        self.filter_mturk_results()
        return self.allUsers

    def return_filtered_users(self):
        """
        Return all the users that have completed the screener questions.

        Filters to users who have provided the desired responses
        """
        self.filter_mturk_results()
        return self.filteredUsers

    def send_reminder_emails(self, users, subj, msg):
        """
        Send a reminder email to a user.
        Appends the WorkerID to the end of the email

        Inputs:
        - users = list of users to receive an email
        - subj = the subject line of the email
        - msg = the body of the message
        """
        result = []
        for user in users:
            try:
                notify = self.mturk.notify_workers(user, subj, msg + user)
                result.append([user, notify])
            except:
                result.append('Could not email user: %s' % user)

        return result

    def send_first_mailer(self):
        """
        The generic format for the first email to be sent.

        Will be sent to all users who have been filtered.
        """
        subject = "Please take second part of Survey for bonus"
        message = """Hello,

        Based on your responses to the screening question,
        you've been selected to participate in the second
        part of the survey.

        Please go to """ + self.srvy_link + """ to complete additional
        questions. At the end of the survey, you will be prompted
        to enter a payment code to verify that you were selected
        to fill out the survey. When asked, please enter the code
        below. Upon completion, you will be awarded a bonus.

        Thanks for your participation!

        Sincerely,
        """ + self.from_name + """

        ### Your reward code is: """

        return self.send_reminder_emails(self.filteredUsers, subject, message)

    def send_second_mailer(self):
        """
        The generic format for the second email to be sent.

        Will be sent only to the filtered users who have NOT yet submitted
        part two of the Survey OR who have incorrectly entered their WorkerID.
        """
        subject = "[Reminder] Bonus for participating in second part of Survey"
        message = """Hello,

        We sent you an email recently about completing additional
        questions for the Mturk Survey. We'd really appreciate
        your time in helping us improve our products further.

        As a reminder, please go to """ + self.srvy_link + """ to complete
        additional questions. At the end of the Survey, you will
        be prompted to enter a payment code to verify that you were
        selected to fill out the survey. When asked, please enter the
        code below. Upon completion, you will be awarded a bonus.

        NOTE: If you are receiving this email, but have already completed
        the Survey, you may have entered the Survey code incorrectly.
        Please redo the survey, ensuring that the code matches the above,
        to receive your compensation.

        Thanks for your participation!

        Sincerely,
        """ + self.from_name + """

        ### Your reward code is: """

        return self.send_reminder_emails(self.remaining, subject, message)

    def award_bonus(self, amount, **kwargs):
        """
        Award a bonus amount to users who completed part 2 of the Survey.

        Input:
        - amount = a dollar amount expressed as a float

        Optional input:
        - customList = a custom list of Worker IDs to send bonuses to. Will
                       override existing completed list and ONLY send to the
                       custom IDs
        - debug = if True, payment(s) not made, instead an informative print
                  element that shows which user(s) get(s) how much bonus; also
                  prints the budget required for the payments
        """
        self.filter_mturk_results()
        self.get_results()

        payment = boto.mturk.connection.Price(amount)
        bonus_message = "Thanks for completing the second part of the Survey!"

        if 'customList' in kwargs:
            workerList = kwargs['customList']
        else:
            workerList = self.completeActual

        budget = 0
        for hit in self.HITlist:

            currentPanda = self.filtered_mturk_resp[
                (self.filtered_mturk_resp['HITID'] == hit)
                & (self.filtered_mturk_resp['WorkerID'].isin(workerList))]
            pandaDict = currentPanda.set_index('WorkerID') \
                                    .to_dict(orient='index')

            for user in pandaDict:

                budget += amount  # keeps track of the total budget required
                if 'debug' in kwargs and kwargs['debug']:
                    print('DEBUG ON:', user, amount)
                else:
                    bonus = self.mturk.grant_bonus(
                        user, pandaDict[user]['AssignmentID'],
                        payment, bonus_message)
                    print(user, bonus)

        if 'debug' in kwargs and kwargs['debug']:
            budget *= 1.2
            print('Total budget required (incl. MTurk fees): $%s' % budget)

    def get_results(self):
        """:return: "results" pandas dataframe."""
        raise NotImplementedError('Implement in subclass')

    def merge(self, csv_fname=None):
        """
        Merges Mturk and Gspread data; saves to csv file if provided

        :param csv_fname: filename for .csv output
        :return: Pandas dataframe object with the two joined files
        """
        # merge the mturk responses with the gspread responses
        self.filter_mturk_results()
        self.get_results()
        self.merged = pd.merge(self.filtered_mturk_resp,
                               self.results,
                               left_on='WorkerID',
                               right_on=self.srvy_q_text)

        # merge drop the joinder column since it's duplicative
        self.merged = self.merged.drop([self.srvy_q_text], axis=1)

        # set new index in place
        self.merged.set_index('WorkerID', inplace=True)

        if csv_fname:
            self.merged.to_csv(csv_fname)
            print ('\nExported to %s' % csv_fname)

        return self.merged

    def return_completed(self):
        """
        Return the WorkerIDs for completed results from both parts of the survey
        """
        self.filter_mturk_results()
        self.get_results()
        return self.completeActual

    def return_remaining(self):
        """
        Return the WorkerIDs for the users who only filled out the Mturk part of the survey
        """
        self.filter_mturk_results()
        self.get_results()
        return self.remaining


class GoogleForms(Survey):
    """
    An extension of the Survey class that implements Google forms.
    """
    def __init__(self, access_key, secret_access_key, HITlist,
                 questions, srvy_link, spreadsheet_id, srvy_q_text, 
                 client_secret, from_name):
        super().__init__(access_key, secret_access_key, HITlist, questions, 
                         srvy_link, from_name)
        self.spreadsheet_id = spreadsheet_id
        self.srvy_q_text = srvy_q_text

        # Setup the Sheets API
        SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
        store = file.Storage('credentials.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(client_secret, SCOPES)
            creds = tools.run_flow(flow, store)
        self.service = build('sheets', 'v4', http=creds.authorize(Http()))

    def get_results(self, spreadsheet_tab_name='Form Responses 1', columns='A:AZ'):
        """
        Fetch results from a specified spreadsheet on Google Sheets

        :param spreadsheet_tab_name: the name of the tab on the Gspread sheet
                                     defaults to `Form Responses 1` since that's
                                     what Google Forms auto-generates
        :param columns: the columns to be grabbed in A1 format; defaults to `A:AZ`
        :return: pandas dataframe containing the spreadsheet data
        """
        SPREADSHEET_ID = self.spreadsheet_id
        RANGE_NAME = '%s!%s' % (spreadsheet_tab_name, columns)
        result = self.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME).execute()
        values = result.get('values', [])
        if not values:
            print('No data found.')
        else:

            # organize data into pandas df
            df = pd.DataFrame(values)
            df.columns = df.iloc[0]
            df = df.reindex(df.index.drop(0))

            # grab desired data
            self.completeList = df[self.srvy_q_text].values.tolist()
            self.completeActual = list(
                set([
                    user for user in self.filteredUsers
                    if user in self.completeList
                    ])
                )
            self.remaining = [
                user for user in self.filteredUsers
                if user not in self.completeActual
            ]
            self.results = pd.DataFrame(df)

            return self.results

        
