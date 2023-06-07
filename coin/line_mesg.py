class line_message:
    def __init__(self,line_key,LT):
        self.line_key = line_key
        self.LT = LT

    def error(self,id,ET,message):
        error_mesg = f"curl -X POST -H 'Authorization: Bearer {self.line_key},' -F 'message={id}-{self.LT}-Execute time:{ET} \n {message}' https://notify-api.line.me/api/notify"
        return error_mesg
    

