const sendButton = document.getElementById('send-button');
const userText = document.getElementById('user-text');
const responseDiv1 = document.getElementById('response1');
const responseDiv2 = document.getElementById('response2');
const responseDiv3 = document.getElementById('response3');


sendButton.addEventListener('click', () => {
    fetch('http://127.0.0.1:5000/dsl', {
        method: 'POST',
        body: JSON.stringify({text: userText.value}),
        headers: {'Content-Type': 'application/json'}
    })
        .then(response => response.json())
        .then(data => {
            console.log("received data", data)
            const image = document.createElement('img');
            image.src = 'data:image/png;base64,' + data.image;
            //responseDiv1.body.appendChild(image);
            if(responseDiv1.firstChild != null){
                console.log("has first", responseDiv1.firstChild)
                responseDiv1.firstChild.textContent = 'Graph';
                responseDiv1.firstChild.nextSibling.replaceWith(image);
                /*responseDiv1.firstChild = image;*/
            }
            else {
                console.log("appending")
                responseDiv1.appendChild(image);
            }
            responseDiv2.innerHTML = data.cycles;
            responseDiv3.innerHTML = data.paths;
        })
        .catch(error => {
            console.error('Error sending data to server:', error);
        });
});